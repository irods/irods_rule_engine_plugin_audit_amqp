#ifndef IRODS_AUDIT_AMQP_PROTON_OBJECT_TYPE_HPP
#define IRODS_AUDIT_AMQP_PROTON_OBJECT_TYPE_HPP

namespace irods::plugin::rule_engine::audit_amqp
{
	enum class proton_object_type
	{
		UNKNOWN,
		ERROR_CONDITION,
		TRACKER,
		TRANSPORT,
		CONNECTION,
		SESSION,
		SENDER
	};
}

#endif // IRODS_AUDIT_AMQP_PROTON_OBJECT_TYPE_HPP
