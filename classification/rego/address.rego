package classifier_address

import rego.v1

output[k] := v if {
	some k in object.keys(input)
	v := classify(k, input[k])
}

default classify(_, _) := "UNLABELED"

classify(key, _) := "ADDRESS" if {
	lower(key) == "state"
}

classify(key, _) := "ADDRESS" if {
	lower(key) == "zip"
}

classify(key, _) := "ADDRESS" if {
	lower(key) == "zipcode"
}

classify(key, _) := "ADDRESS" if {
	lower(key) == "zipcode"
}

classify(key, _) := "ADDRESS" if {
	regex.match(`\A.*address.*\z`, lower(key))
}

classify(key, _) := "ADDRESS" if {
	regex.match(`\Astreet.*\z`, lower(key))
}
