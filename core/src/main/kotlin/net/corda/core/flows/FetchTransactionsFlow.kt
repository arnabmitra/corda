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
@InitiatingFlow
class FetchTransactionsFlow(requests: Set<SecureHash>, otherSide: Party) :
        FetchDataFlow<SignedTransaction, SignedTransaction>(requests, otherSide, SignedTransaction::class.java) {

    override fun load(txid: SecureHash): SignedTransaction? = serviceHub.validatedTransactions.getTransaction(txid)
}

/**
 * Given a set of hashes either loads from from local storage  or requests them from the other peer. Downloaded
 * attachments are saved to local storage automatically.
 */
class FetchAttachmentsFlow(requests: Set<SecureHash>, otherSide: Party) : FetchDataFlow<Attachment, ByteArray>(requests, otherSide, ByteArray::class.java) {
    override fun load(txid: SecureHash): Attachment? = serviceHub.attachments.openAttachment(txid)
    override fun convert(wire: ByteArray): Attachment = FetchedAttachment({ wire })
    override fun maybeWriteToDisk(downloaded: List<Attachment>) {
        for (attachment in downloaded) {
            serviceHub.attachments.importAttachment(attachment.open())
        }
    }

    private class FetchedAttachment(dataLoader: () -> ByteArray) : AbstractAttachment(dataLoader), SerializeAsToken {
        override val id: SecureHash by lazy { attachmentData.sha256() }

        private class Token(private val id: SecureHash) : SerializationToken {
            override fun fromToken(context: SerializeAsTokenContext) = FetchedAttachment(context.attachmentDataLoader(id))
        }

        override fun toToken(context: SerializeAsTokenContext) = Token(id)
    }
}
