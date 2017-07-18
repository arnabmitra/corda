package net.corda.core.serialization.carpenter

import net.corda.core.serialization.amqp.Schema as AMQPSchema
import net.corda.core.serialization.amqp.Field as AMQPField
import net.corda.core.serialization.amqp.CompositeType

fun AMQPField.getTypeAsClass(classLoaders: List<ClassLoader>) =
        mutable.getTypeAsClass(classLoaders, type, requires.getOrElse(0){""})

fun AMQPSchema.carpenterSchema(
        loaders : List<ClassLoader> = listOf<ClassLoader>(ClassLoader.getSystemClassLoader()))
        : CarpenterSchemas {
    val rtn = CarpenterSchemas.newInstance()

    types.filterIsInstance<CompositeType>().forEach {
        it.carpenterSchema(classLoaders = loaders, carpenterSchemas = rtn)
    }

    return rtn
}

/**
 * if we can load the class then we MUST know about all of it's composite elements
 */
private fun CompositeType.validatePropertyTypes(
        classLoaders: List<ClassLoader> = listOf<ClassLoader> (ClassLoader.getSystemClassLoader())){
    fields.forEach {
        if (!it.validateType(classLoaders)) throw UncarpentableException (name, it.name, it.type)
    }
}

/**
 * based upon this AMQP schema either
 *  a) add the corresponding carpenter schema to the [carpenterSchemas] param
 *  b) add the class to the dependency tree in [carpenterSchemas] if it cannot be instantiated
 *     at this time
 *
 *  @param classLoaders list of classLoaders, defaulting toe the system class loader, that might
 *  be used to load objects
 *  @param carpenterSchemas structure that holds the dependency tree and list of classes that
 *  need constructing
 *  @param force by default a schema is not added to [carpenterSchemas] if it already exists
 *  on the class path. For testing purposes schema generation can be forced
 */
fun CompositeType.carpenterSchema(
        classLoaders: List<ClassLoader> = listOf<ClassLoader> (ClassLoader.getSystemClassLoader()),
        carpenterSchemas : CarpenterSchemas,
        force : Boolean = false) {
    if (classLoaders.exists(name)) {
        validatePropertyTypes(classLoaders)
        if (!force) return
    }

    val providesList = mutableListOf<Class<*>>()

    var isInterface = false
    var isCreatable = true

    provides.forEach {
        if (name == it) {
            isInterface = true
            return@forEach
        }

        try {
            providesList.add (classLoaders.loadIfExists(it))
        }
        catch (e: ClassNotFoundException) {
            carpenterSchemas.addDepPair(this, name, it)
            isCreatable = false
        }
    }

    val m : MutableMap<String, Field> = mutableMapOf()

    fields.forEach {
        try {
            m[it.name] = FieldFactory.newInstance(it.mandatory, it.name, it.getTypeAsClass(classLoaders))
        }
        catch (e: ClassNotFoundException) {
            carpenterSchemas.addDepPair(this, name, it.typeAsString())
            isCreatable = false
        }
    }

    if (isCreatable) {
        carpenterSchemas.carpenterSchemas.add (CarpenterSchemaFactory.newInstance(
                name = name,
                fields = m,
                interfaces = providesList,
                isInterface = isInterface))
    }
}

fun AMQPField.validateType(
        classLoaders: List<ClassLoader> = listOf<ClassLoader> (ClassLoader.getSystemClassLoader())
) = when (type) {
    "int", "string", "short", "long", "char", "boolean", "double", "float" -> true
    "*"  -> classLoaders.exists(requires[0])
    else -> classLoaders.exists (type)
}

private fun List<ClassLoader>.exists (clazz: String) =
        this.find { try { it.loadClass(clazz); true } catch (e: ClassNotFoundException) { false } } != null

private fun List<ClassLoader>.loadIfExists (clazz: String) : Class<*> {
    this.forEach {
        try {
            return it.loadClass(clazz)
        } catch (e: ClassNotFoundException) {
            return@forEach
        }
    }
    throw ClassNotFoundException(clazz)
}
