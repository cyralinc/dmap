package classifier_dob

import rego.v1

test_dob_key if {
	output.dob == true with input as {"dob":"test"}
}

test_dob_key if {
	output.DOB == true with input as {"DOB":"test"}
}

test_dob_key if {
	output.DoB == true with input as {"DoB":"test"}
}

test_dob_key if {
	output.birthdate == true with input as {"birthdate":"test"}
}

test_dob_key if {
	output.BirthDate == true with input as {"BirthDate":"test"}
}

test_dob_key if {
	output.dateofbirth == true with input as {"dateofbirth":"test"}
}

test_dob_key if {
	output.DateOfBirth == true with input as {"DateOfBirth":"test"}
}

test_dob_key if {
	output.birthdate == true with input as {"birthdate":"test"}
}

test_dob_key if {
	output.birth_date == true with input as {"birth_date":"test"}
}

# mm/dd/yyyy

test_dob_pattern if {
	output.message == true with input as {"message":"01/01/1900"}
}

test_dob_pattern if {
	output.message == true with input as {"message":"1-1-1900"}
}

test_dob_pattern if {
	output.message == true with input as {"message":"10.01.1971"}
}

test_dob_pattern if {
	output.message == true with input as {"message":"11/30/2023"}
}

# dd/mm/yyyy

test_dob_pattern if {
	output.message == true with input as {"message":"01/01/1900"}
}

test_dob_pattern if {
	output.message == true with input as {"message":"1/1/1900"}
}

test_dob_pattern if {
	output.message == true with input as {"message":"10/01/1971"}
}

test_dob_pattern if {
	output.message == true with input as {"message":"30/11/2023"}
}


# yyyy/mm/dd

test_dob_pattern if {
	output.message == true with input as {"message":"1900/01/10"}
}

test_dob_pattern if {
	output.message == true with input as {"message":"1900/1/1"}
}

test_dob_pattern if {
	output.message == true with input as {"message":"1971/10/01"}
}

test_dob_pattern if {
	output.message == true with input as {"message":"2023/12/31"}
}

# yyyy-mm-dd

test_dob_pattern if {
	output.message == true with input as {"message":"1900-01-10"}
}

test_dob_pattern if {
	output.message == true with input as {"message":"1900-1-1"}
}

test_dob_pattern if {
	output.message == true with input as {"message":"1971-10-01"}
}

test_dob_pattern if {
	output.message == true with input as {"message":"2023-12-31"}
}
