package classifier_dob

import rego.v1

output[k] := v if {
	some k in object.keys(input)
	v := classify(k, input[k])
}

default classify(_, _) := "UNLABELED"

classify(key, _) := "DOB" if {
	lower(key) == "dob"
}

classify(key, _) := "DOB" if {
	lower(key) == "dateofbirth"
}

classify(key, _) := "DOB" if {
	lower(key) == "date_of_birth"
}

classify(key, _) := "DOB" if {
	lower(key) == "birthdate"
}

classify(key, _) := "DOB" if {
	lower(key) == "birth_date"
}

classify(_, val) := "DOB" if {
	# mm/dd/yyyy mm-dd-yyyy mm.dd.yyyy
	regex.match(`^(0?[1-9]|1[0-2])[\/\.-](0?[1-9]|[12]\d|3[01])[\/\.-](19|20)\d{2}$`, val)
}

classify(_, val) := "DOB" if {
	# dd/mm/yyyy
	regex.match(`^(0?[1-9]|[12]\d|3[01])[\/\.-](0?[1-9]|1[0-2])[\/\.-](19|20)\d{2}$`, val)
}

classify(_, val) := "DOB" if {
	# yyyy/mm/dd
	regex.match(`^(19|20)\d{2}[\/\.-](0?[1-9]|1[0-2])[\/\.-](0?[1-9]|[12]\d|3[01])$`, val)
}
