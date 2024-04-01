package classifier_dob

import rego.v1

# METADATA
# entrypoint: true
output[k] := v if {
	some k in object.keys(input)
	v := classify(k, input[k])
}

default classify(_, _) := false

classify(key, _) if {
	lower(key) == "dob"
}

classify(key, _) if {
	lower(key) == "dateofbirth"
}

classify(key, _) if {
	lower(key) == "date_of_birth"
}

classify(key, _) if {
	lower(key) == "birthdate"
}

classify(key, _) if {
	lower(key) == "birth_date"
}

classify(_, val) if {
	# mm/dd/yyyy mm-dd-yyyy mm.dd.yyyy
	regex.match(`^(0?[1-9]|1[0-2])[\/\.-](0?[1-9]|[12]\d|3[01])[\/\.-](19|20)\d{2}$`, val)
}

classify(_, val) if {
	# dd/mm/yyyy
	regex.match(`^(0?[1-9]|[12]\d|3[01])[\/\.-](0?[1-9]|1[0-2])[\/\.-](19|20)\d{2}$`, val)
}

classify(_, val) if {
	# yyyy/mm/dd
	regex.match(`^(19|20)\d{2}[\/\.-](0?[1-9]|1[0-2])[\/\.-](0?[1-9]|[12]\d|3[01])$`, val)
}
