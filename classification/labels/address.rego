package classifier_address

import rego.v1

# METADATA
# entrypoint: true
output[k] := v if {
	some k in object.keys(input)
	v := classify(k, input[k])
}

default classify(_, _) := false

classify(key, _) if {
	lower(key) == "state"
}

classify(key, _) if {
	lower(key) == "zip"
}

classify(key, _) if {
	contains(lower(key), "zip")
	contains(lower(key), "code")
}

classify(key, _) if {
	contains(lower(key), "postal")
	contains(lower(key), "code")
}

classify(key, _) if {
	regex.match(`\A.*address.*\z`, lower(key))
}

classify(key, _) if {
	regex.match(`\Astreet.*\z`, lower(key))
}
