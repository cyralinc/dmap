package classifier_passport

import rego.v1

output[k] := v if {
	some k in object.keys(input)
	v := classify(k, input[k])
}

default classify(_, _) := "UNLABELED"

classify(key, _) := "PASSPORT" if {
	contains(lower(key), "passport")
}

classify(_, val) := "PASSPORT" if {
	regex.match(`^[A-PR-WYZ]{1,2}[1-9]\d\s?\d{4,6}[1-9]$`, val)
}

classify(_, val) := "PASSPORT" if {
	regex.match(`^[1-9]\d\s?\d{4,6}[1-9][A-PR-WY]{1,2}$`, val)
}
