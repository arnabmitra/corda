package net.corda.core.transactions

import net.corda.core.contracts.*
import net.corda.core.crypto.SecureHash
import net.corda.core.identity.Party
import net.corda.core.serialization.CordaSerializable

/**
 * A LedgerTransaction is derived from a [WireTransaction]. It is the result of doing the following operations:
 *
 * - Downloading and locally storing all the dependencies of the transaction.
 * - Resolving the input states and loading them into memory.
 * - Doing some basic key lookups on the [Command]s to see if any keys are from a recognised party, thus converting the
 *   [Command] objects into [AuthenticatedObject].
 * - Deserialising the output states.
 *
 * All the above refer to inputs using a (txhash, output index) pair.
 */
// TODO LedgerTransaction is not supposed to be serialisable as it references attachments, etc. The verification logic
// currently sends this across to out-of-process verifiers. We'll need to change that first.
@CordaSerializable
data class LedgerTransaction(
        /** The resolved input states which will be consumed/invalidated by the execution of this transaction. */
        override val inputs: List<StateAndRef<ContractState>>,
        override val outputs: List<TransactionState<ContractState>>,
        /** Arbitrary data passed to the program of each input state. */
        val commands: List<AuthenticatedObject<CommandData>>,
        /** A list of [Attachment] objects identified by the transaction that are needed for this transaction to verify. */
        val attachments: List<Attachment>,
        /** The hash of the original serialised WireTransaction. */
        override val id: SecureHash,
        override val notary: Party?,
        val timeWindow: TimeWindow?,
        val type: TransactionType
) : FullTransaction {
    init {
        check(notary != null || timeWindow == null) { "Transactions with time-windows must be notarised" }
        checkInputsHaveSameNotary()
        checkNoNotaryChange()
        checkEncumbrancesValid()
    }

    /**
     * Verifies this transaction and runs contract code. At this stage it is assumed that signatures have already been verified.
     *
     * @throws TransactionVerificationException if anything goes wrong.
     */
    @Throws(TransactionVerificationException::class)
    fun verify() = verifyContracts()

    /**
     * Check the transaction is contract-valid by running the verify() for each input and output state contract.
     * If any contract fails to verify, the whole transaction is considered to be invalid.
     */
    private fun verifyContracts() {
        val ctx = toTransactionForContract()
        // TODO: This will all be replaced in future once the sandbox and contract constraints work is done.
        val contracts = (ctx.inputs.map { it.contract } + ctx.outputs.map { it.contract }).toSet()
        for (contract in contracts) {
            try {
                contract.verify(ctx)
            } catch(e: Throwable) {
                throw TransactionVerificationException.ContractRejection(id, contract, e)
            }
        }
    }

    // TODO: Remove this concept.
    // There isn't really a good justification for hiding this data from the contract, it's just a backwards compat hack.
    /** Strips the transaction down to a form that is usable by the contract verify functions */
    private fun toTransactionForContract(): TransactionForContract {
        return TransactionForContract(
                inputs.map { it.state.data },
                outputs.map { it.data },
                attachments,
                commands,
                id,
                notary,
                timeWindow)
    }

    /**
     * Make sure the notary has stayed the same. As we can't tell how inputs and outputs connect, if there
     * are any inputs, all outputs must have the same notary.
     *
     * TODO: Is that the correct set of restrictions? May need to come back to this, see if we can be more
     *       flexible on output notaries.
     */
    private fun checkNoNotaryChange() {
        if (notary != null && inputs.isNotEmpty()) {
            outputs.forEach {
                if (it.notary != notary) {
                    throw TransactionVerificationException.NotaryChangeInWrongTransactionType(id, notary, it.notary)
                }
            }
        }
    }

    private fun checkEncumbrancesValid() {
        // Validate that all encumbrances exist within the set of input states.
        val encumberedInputs = inputs.filter { it.state.encumbrance != null }
        encumberedInputs.forEach { (state, ref) ->
            val encumbranceStateExists = inputs.any {
                it.ref.txhash == ref.txhash && it.ref.index == state.encumbrance
            }
            if (!encumbranceStateExists) {
                throw TransactionVerificationException.TransactionMissingEncumbranceException(
                        id,
                        state.encumbrance!!,
                        TransactionVerificationException.Direction.INPUT
                )
            }
        }

        // Check that, in the outputs, an encumbered state does not refer to itself as the encumbrance,
        // and that the number of outputs can contain the encumbrance.
        for ((i, output) in outputs.withIndex()) {
            val encumbranceIndex = output.encumbrance ?: continue
            if (encumbranceIndex == i || encumbranceIndex >= outputs.size) {
                throw TransactionVerificationException.TransactionMissingEncumbranceException(
                        id,
                        encumbranceIndex,
                        TransactionVerificationException.Direction.OUTPUT)
            }
        }
    }
}
