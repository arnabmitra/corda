package net.corda.core.flows

import co.paralleluniverse.fibers.Suspendable
import net.corda.core.crypto.SecureHash
import net.corda.core.getOrThrow
import net.corda.core.identity.Party
import net.corda.core.serialization.CordaSerializable
import net.corda.core.transactions.LedgerTransaction
import net.corda.core.transactions.SignedTransaction
import net.corda.core.transactions.WireTransaction
import net.corda.core.utilities.UntrustworthyData
import net.corda.core.utilities.unwrap
import java.util.*
import kotlin.collections.LinkedHashSet

// TODO: It may be a clearer API if we make the primary c'tor private here, and only allow a single tx to be "resolved".

/**
 * The [ResolveTransactionsFlow] corresponds to the [SendTransactionFlow].
 *
 * This flow is used to verify the validity of a transaction by recursively checking the validity of all the
 * dependencies. Once a transaction is checked it's inserted into local storage so it can be relayed and won't be
 * checked again.
 *
 * A couple of constructors are provided that accept a single transaction. When these are used, the dependencies of that
 * transaction are resolved and then the transaction itself is verified. Again, if successful, the results are inserted
 * into the database as long as a [SignedTransaction] was provided. If only the [WireTransaction] form was provided
 * then this isn't enough to put into the local database, so only the dependencies are checked and inserted. This way
 * to use the flow is helpful when resolving and verifying a finished but partially signed transaction.
 *
 * The flow returns a list of verified [LedgerTransaction] objects, in a depth-first order.
 */
class ResolveTransactionsFlow(private val otherSide: Party, private val transactionData: ResolvableTransactionData,
                              private val verifySignatures: Boolean = true, private val verifyTransaction: Boolean = true) : FlowLogic<List<LedgerTransaction>>() {
    constructor(otherSide: Party, transactionData: ResolvableTransactionData) : this(otherSide, transactionData, true, true)

    companion object {
        /**
         * Topologically sorts the given transactions such that dependencies are listed before dependers. */
        @JvmStatic
        fun topologicalSort(transactions: Collection<SignedTransaction>): List<SignedTransaction> {
            // Construct txhash -> dependent-txs map
            val forwardGraph = transactions.flatMap { stx -> stx.tx.inputs.map { it.txhash to stx } }
                    .groupBy { it.first }
                    // Note that we use a LinkedHashSet here to make the traversal deterministic (as long as the input list is)
                    .mapValues { LinkedHashSet(it.value.map { it.second }) }

            val visited = HashSet<SecureHash>(transactions.size)
            val result = ArrayList<SignedTransaction>(transactions.size)

            fun visit(transaction: SignedTransaction) {
                if (transaction.id !in visited) {
                    visited.add(transaction.id)
                    forwardGraph[transaction.id]?.forEach(::visit)
                    result.add(transaction)
                }
            }
            transactions.forEach(::visit)
            result.reverse()
            require(result.size == transactions.size)
            return result
        }
    }

    @CordaSerializable
    class ExcessivelyLargeTransactionGraph : Exception()

    // TODO: Figure out a more appropriate DOS limit here, 5000 is simply a very bad guess.
    /** The maximum number of transactions this flow will try to download before bailing out. */
    var transactionCountLimit = 5000

    @Suspendable
    @Throws(FetchDataFlow.HashNotFound::class)
    override fun call(): List<LedgerTransaction> {
        val stx = when (transactionData) {
            is SignedTransaction -> transactionData
            is ResolvableTransactionData.Transaction -> transactionData.stx
            else -> null
        }

        val wtx = if (verifySignatures) {
            stx?.verifyRequiredSignatures()
        } else {
            stx?.tx
        }

        // Start fetching data.
        val newTxns = downloadDependencies(transactionData.dependencies)
        fetchMissingAttachments(newTxns.flatMap { it.tx.attachments } + (wtx?.attachments ?: emptyList()))
        send(otherSide, FetchDataFlow.Request.End)
        // Finish fetching data.

        val result = topologicalSort(newTxns).map {
            // For each transaction, verify it and insert it into the database. As we are iterating over them in a
            // depth-first order, we should not encounter any verification failures due to missing data. If we fail
            // half way through, it's no big deal, although it might result in us attempting to re-download data
            // redundantly next time we attempt verification.
            val ltx = it.toLedgerTransaction(serviceHub)
            serviceHub.transactionVerifierService.verify(ltx).getOrThrow()
            serviceHub.recordTransactions(it)
            ltx
        }
        return if (wtx == null || !verifyTransaction) {
            result
        } else {
            result + wtx.toLedgerTransaction(serviceHub).apply { verify() }
        }
    }

    @Suspendable
    private fun downloadDependencies(depsToCheck: Set<SecureHash>): List<SignedTransaction> {
        // Maintain a work queue of all hashes to load/download, initialised with our starting set. Then do a breadth
        // first traversal across the dependency graph.
        //
        // TODO: This approach has two problems. Analyze and resolve them:
        //
        // (1) This flow leaks private data. If you download a transaction and then do NOT request a
        // dependency, it means you already have it, which in turn means you must have been involved with it before
        // somehow, either in the tx itself or in any following spend of it. If there were no following spends, then
        // your peer knows for sure that you were involved ... this is bad! The only obvious ways to fix this are
        // something like onion routing of requests, secure hardware, or both.
        //
        // (2) If the identity service changes the assumed identity of one of the public keys, it's possible
        // that the "tx in db is valid" invariant is violated if one of the contracts checks the identity! Should
        // the db contain the identities that were resolved when the transaction was first checked, or should we
        // accept this kind of change is possible? Most likely solution is for identity data to be an attachment.

        val nextRequests = LinkedHashSet<SecureHash>()   // Keep things unique but ordered, for unit test stability.
        nextRequests.addAll(depsToCheck)
        val resultQ = LinkedHashMap<SecureHash, SignedTransaction>()

        var limitCounter = transactionCountLimit
        check(limitCounter > 0) { "$limitCounter is not a valid count limit" }

        while (nextRequests.isNotEmpty()) {
            if (limitCounter < 0) throw ExcessivelyLargeTransactionGraph()
            limitCounter -= nextRequests.size

            // Don't re-download the same tx when we haven't verified it yet but it's referenced multiple times in the
            // graph we're traversing.
            val notAlreadyFetched = nextRequests.filterNot { it in resultQ }.toSet()
            if (notAlreadyFetched.isEmpty())
                break // Done early.

            // Request the standalone transaction data (which may refer to things we don't yet have).
            val downloads = subFlow(FetchTransactionsFlow(notAlreadyFetched, otherSide)).downloaded

            for (stx in downloads)
                check(resultQ.putIfAbsent(stx.id, stx) == null)   // Assert checks the filter at the start.

            // Add all input states to the work queue.
            val inputHashes = downloads.flatMap { it.tx.inputs }.map { it.txhash }

            nextRequests.clear()
            nextRequests.addAll(inputHashes)
        }
        return resultQ.values.toList()
    }

    /**
     * Returns a list of all the dependencies of the given transactions, deepest first i.e. the last downloaded comes
     * first in the returned list and thus doesn't have any unverified dependencies.
     */
    @Suspendable
    private fun fetchMissingAttachments(downloads: List<SecureHash>) {
        // TODO: This could be done in parallel with other fetches for extra speed.
        val missingAttachments = downloads.filter { serviceHub.attachments.openAttachment(it) == null }
        if (missingAttachments.isNotEmpty())
            subFlow(FetchAttachmentsFlow(missingAttachments.toSet(), otherSide))
    }
}

@CordaSerializable
interface ResolvableTransactionData {
    val dependencies: Set<SecureHash>

    interface Transaction : ResolvableTransactionData {
        override val dependencies: Set<SecureHash> get() = stx.dependencies
        val stx: SignedTransaction
    }

    data class TransactionHashes(override val dependencies: Set<SecureHash>) : ResolvableTransactionData
}