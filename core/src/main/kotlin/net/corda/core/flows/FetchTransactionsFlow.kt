package net.corda.core.flows

import net.corda.core.contracts.AbstractAttachment
import net.corda.core.contracts.Attachment
import net.corda.core.crypto.SecureHash
import net.corda.core.crypto.sha256
import net.corda.core.identity.Party
import net.corda.core.serialization.SerializationToken
import net.corda.core.serialization.SerializeAsToken
import net.corda.core.serialization.SerializeAsTokenContext
import net.corda.core.transactions.SignedTransaction

/**
 * Given a set of tx hashes (IDs), either loads them from local disk or asks the remote peer to provide them.
 *
 * A malicious response in which the data provided by the remote peer does not hash to the requested hash results in
 * [FetchDataFlow.DownloadedVsRequestedDataMismatch] being thrown. If the remote peer doesn't have an entry, it
 * results in a [FetchDataFlow.HashNotFound] exception. Note that returned transactions are not inserted into
 * the database, because it's up to the caller to actually verify the transactions are valid.
 */
class FetchTransactionsFlow(requests: Set<SecureHash>, otherSide: Party) :
        FetchDataFlow<SignedTransaction, SignedTransaction>(requests, otherSide, DataType.TRANSACTION) {

    override fun load(txid: SecureHash): SignedTransaction? = serviceHub.validatedTransactions.getTransaction(txid)
}
