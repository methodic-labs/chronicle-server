package com.openlattice.chronicle.util.tests

import com.geekbeast.rhizome.hazelcast.DelegatedIntList
import com.openlattice.chronicle.authorization.AclKeySet
import com.openlattice.chronicle.serializers.AclKeyStreamSerializer
import org.apache.commons.text.CharacterPredicates
import org.apache.commons.text.RandomStringGenerator
import kotlin.random.Random

class InternalTestDataFactory {
    companion object {
        private val allowedLetters = arrayOf(charArrayOf('a', 'z'), charArrayOf('A', 'Z'))
        private val allowedDigitsAndLetters =
            arrayOf(charArrayOf('a', 'z'), charArrayOf('A', 'Z'), charArrayOf('0', '9'))
        private val random = RandomStringGenerator.Builder()
            .build()
        private val randomAlpha = RandomStringGenerator.Builder()
            .withinRange(*allowedLetters)
            .filteredBy(CharacterPredicates.LETTERS)
            .build()
        private val randomAlphaNumeric = RandomStringGenerator.Builder()
            .withinRange(*allowedDigitsAndLetters)
            .filteredBy(CharacterPredicates.LETTERS, CharacterPredicates.DIGITS)
            .build()


        @JvmStatic
        fun delegatedIntList(): DelegatedIntList {
            return DelegatedIntList(
                listOf(
                    Random.nextInt(), Random.nextInt(), Random.nextInt(), Random.nextInt(), Random.nextInt(),
                    Random.nextInt(), Random.nextInt(), Random.nextInt(), Random.nextInt(), Random.nextInt()
                )
            )
        }

        @JvmStatic
        fun aclKeySet(): AclKeySet {
            return AclKeySet(
                mutableSetOf(
                    AclKeyStreamSerializer().generateTestValue(),
                    AclKeyStreamSerializer().generateTestValue(),
                    AclKeyStreamSerializer().generateTestValue(),
                    AclKeyStreamSerializer().generateTestValue(),
                    AclKeyStreamSerializer().generateTestValue(),
                    AclKeyStreamSerializer().generateTestValue(),
                    AclKeyStreamSerializer().generateTestValue()
                )
            )
        }
    }

}