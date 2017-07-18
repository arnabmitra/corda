package net.corda.core.flows

import co.paralleluniverse.fibers.Suspendable
import net.corda.core.identity.Party
import net.corda.core.utilities.UntrustworthyData
import net.corda.core.utilities.unwrap

/**
 * The [SendTransactionFlow] corresponds to the [ReceiveTransactionFlow].
 *
 * The [SendTransactionFlow] provides an ad hoc data vending service, which anticipates incoming data request from the
 * [otherSide] during the transaction resolving process.
 *
 * The number of request from [ReceiveTransactionFlow] is depends on the depth of the transaction history and the data
 * [otherSide] already possess. The [SendTransactionFlow] is expected to receive [FetchDataFlow.Request] continuously
 * until the [otherSide] has all the data they need to resolve the transaction, an [FetchDataFlow.Request.End] will be
 * sent from the [otherSide] to indicate end of data request.
 *
 * @param otherSide the target party.
 * @param payload the message that will be sent to the [otherSide] before data vending starts.
 */
open class SendTransactionFlow(protected val otherSide: Party, protected val data: ResolvableTransactionData?) : FlowLogic<Unit>() {
    @Suspendable
    protected open fun sendPayloadAndReceiveDataRequest(payload: Any?) = payload?.let { sendAndReceive<FetchDataFlow.Request>(otherSide, payload) } ?: receive<FetchDataFlow.Request>(otherSide)

    @Suspendable
    protected open fun verifyDataRequest(dataRequest: FetchDataFlow.Request.Data) {
        // TODO: Verify request is relevant to the transaction.
    }

    @Suspendable
    override fun call() {
        // The first payload will be the transaction data, subsequent payload will be the transaction/attachment data.
        var payload: Any? = data
        // This loop will receive [FetchDataFlow.Request] continuously until the `otherSide` has all the data they need
        // to resolve the transaction, a [FetchDataFlow.EndRequest] will be sent from the `otherSide` to indicate end of
        // data request.
        while (true) {
            val dataRequest = sendPayloadAndReceiveDataRequest(payload).unwrap { request ->
                when (request) {
                    is FetchDataFlow.Request.Data -> {
                        // Verify request.
                        if (request.hashes.isEmpty()) throw FlowException("Empty hash list")
                        verifyDataRequest(request)
                        request
                    }
                    FetchDataFlow.Request.End -> return
                }
            }
            payload = when (dataRequest.dataType) {
                FetchDataFlow.DataType.TRANSACTION -> dataRequest.hashes.map {
                    serviceHub.validatedTransactions.getTransaction(it) ?: throw FetchDataFlow.HashNotFound(it)
                }
                FetchDataFlow.DataType.ATTACHMENT -> dataRequest.hashes.map {
                    serviceHub.attachments.openAttachment(it)?.open()?.readBytes() ?: throw FetchDataFlow.HashNotFound(it)
                }
            }
        }
    }
}

// Convenient methods for Kotlin.
fun FlowLogic<*>.sendTransaction(otherSide: Party, data: ResolvableTransactionData?) = subFlow(SendTransactionFlow(otherSide, data))

inline fun <reified T : Any> FlowLogic<*>.sendTransactionAndReceive(otherSide: Party, data: ResolvableTransactionData?): UntrustworthyData<T> {
    subFlow(SendTransactionFlow(otherSide, data))
    return receive(otherSide)
}