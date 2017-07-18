package net.corda.core.transactions

import net.corda.core.contracts.*
import net.corda.core.identity.Party
import net.corda.core.indexOfOrThrow

/**
 * A transaction with the minimal amount of information required to compute the unique transaction [id], and
 * resolve a [FullTransaction]. This type of transaction, wrapped in [SignedTransaction], gets transferred across the
 * wire and recorded to storage.
 */
interface CoreTransaction : NamedByHash {
    /** The inputs of this transaction, containing state references only **/
    val inputs: List<StateRef>

    /**
     * If present, the notary for this transaction. If absent then the transaction is not notarised at all.
     * This is intended for issuance/genesis transactions that don't consume any other states and thus can't
     * double spend anything.
     */
    val notary: Party?

    /** Throws an exception if duplicate inputs detected */
    fun checkNoDuplicateInputs() {
        val duplicates = inputs.groupBy { it }.filter { it.value.size > 1 }.keys
        check(duplicates.isEmpty()) { "Duplicate input states detected" }
    }
}

/** A transaction with fully resolved components, such as input states. */
interface FullTransaction : NamedByHash {
    val inputs: List<StateAndRef<ContractState>>
    val outputs: List<TransactionState<ContractState>>
    val notary: Party?

    fun checkInputsHaveSameNotary() {
        if (inputs.isEmpty()) return
        val inputNotaries = inputs.map { it.state.notary }.toHashSet()
        check(inputNotaries.size == 1) { "All inputs must point to the same notary" }
        check(inputNotaries.single() == notary) { "The specified notary must be the one specified by all inputs" }
    }

    @Suppress("UNCHECKED_CAST")
    fun <T : ContractState> outRef(index: Int) = StateAndRef(outputs[index] as TransactionState<T>, StateRef(id, index))

    /** Returns a [StateAndRef] for the requested output state, or throws [IllegalArgumentException] if not found. */
    fun <T : ContractState> outRef(state: ContractState): StateAndRef<T> = outRef(outputs.map { it.data }.indexOfOrThrow(state))
}