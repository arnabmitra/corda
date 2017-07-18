package net.corda.core.flows

import co.paralleluniverse.fibers.Suspendable
import net.corda.core.identity.Party
import net.corda.core.utilities.UntrustworthyData
import net.corda.core.utilities.unwrap

/**
 * The [ReceiveTransactionFlow] corresponds to the [SendTransactionFlow]
 *
 * This flow is a combination of [receive] and [ResolveTransactionsFlow], it will expect a incoming message of type [T],
 * which is a sub class of [ResolvableTransactionData]. This flow will resolve the transaction data and return a [UntrustworthyData]
 * for further verification.
 */
class ReceiveTransactionFlow<T : ResolvableTransactionData>(private val expectedTXClazz: Class<T>,
                                                            private val otherSide: Party,
                                                            private val verifySignatures: Boolean,
                                                            private val verifyTransaction: Boolean) : FlowLogic<UntrustworthyData<T>>() {
    constructor(expectedTXClazz: Class<T>, otherSide: Party) : this(expectedTXClazz, otherSide, true, true)

    @Suspendable
    override fun call(): UntrustworthyData<T> {
        return receive(expectedTXClazz, otherSide).unwrap {
            subFlow(ResolveTransactionsFlow(otherSide, it, verifySignatures, verifyTransaction))
            UntrustworthyData(it)
        }
    }
}

// Helper method for Kotlin.
inline fun <reified T : ResolvableTransactionData> FlowLogic<*>.receiveTransaction(otherSide: Party,
                                                                                   verifySignature: Boolean = true,
                                                                                   verifyTransaction: Boolean = true)
        = subFlow(ReceiveTransactionFlow(T::class.java, otherSide, verifySignature, verifyTransaction))

