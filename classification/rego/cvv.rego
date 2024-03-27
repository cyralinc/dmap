package classifier_cvv

import rego.v1

output[k] := v if {
	some k in object.keys(input)
	v := classify(k, input[k])
}

default classify(_, _) := false

classify(key, _) if {
	lower(key) == "cvv"
}

classify(key, _) if {
	lower(key) == "csc"
}

classify(key, _) if {
	lower(key) == "3csc"
}

classify(key, _) if {
	lower(key) == "cvc"
}

classify(key, _) if {
	lower(key) == "cav"
}

classify(key, _) if {
	lower(key) == "cid"
}

classify(key, _) if {
	lower(key) == "cvd"
}

classify(key, _) if {
	lower(key) == "cve"
}

classify(key, _) if {
	lower(key) == "cvn"
}

classify(key, _) if {
	lower(key) == "spc"
}

classify(key, _) if {
	lower(key) == "pvv"
}

classify(_, val) if {
	regex.match(`^\d{3,4}$`, val)
}
