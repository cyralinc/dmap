package classifier_address

import rego.v1

# METADATA
# entrypoint: true
output[k] := v if {
	some k in object.keys(input)
	v := classify(k)
}

default classify(_) := false

classify(key) if {
	lower(key) == "state"
}

classify(key) if {
	lower(key) == "zip"
}

classify(key) if {
	contains(lower(key), "zip")
	contains(lower(key), "code")
}

classify(key) if {
	contains(lower(key), "postal")
	contains(lower(key), "code")
}

classify(key) if {
	regex.match(`\A.*address.*\z`, lower(key))
}

classify(key) if {
	regex.match(`\Astreet.*\z`, lower(key))
}
