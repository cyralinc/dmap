package classifier_last_name

import rego.v1

# METADATA
# entrypoint: true
output[k] := v if {
	some k in object.keys(input)
	v := classify(k)
}

default classify(_) := false

classify(key) if {
	contains(lower(key), "last")
	contains(lower(key), "name")
}

classify(key) if {
	contains(lower(key), "sur")
	contains(lower(key), "name")
}
