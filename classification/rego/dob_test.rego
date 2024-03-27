package classifier_dob

test_dob_key {
	output.dob == "DOB" with input as {"dob":"test"}
}

test_dob_key {
	output.DOB == "DOB" with input as {"DOB":"test"}
}

test_dob_key {
	output.DoB == "DOB" with input as {"DoB":"test"}
}

test_dob_key {
	output.birthdate == "DOB" with input as {"birthdate":"test"}
}

test_dob_key {
	output.BirthDate == "DOB" with input as {"BirthDate":"test"}
}

test_dob_key {
	output.dateofbirth == "DOB" with input as {"dateofbirth":"test"}
}

test_dob_key {
	output.DateOfBirth == "DOB" with input as {"DateOfBirth":"test"}
}

test_dob_key {
	output.birthdate == "DOB" with input as {"birthdate":"test"}
}

test_dob_key {
	output.birth_date == "DOB" with input as {"birth_date":"test"}
}

# mm/dd/yyyy

test_dob_pattern {
	output.message == "DOB" with input as {"message":"01/01/1900"}
}

test_dob_pattern {
	output.message == "DOB" with input as {"message":"1-1-1900"}
}

test_dob_pattern {
	output.message == "DOB" with input as {"message":"10.01.1971"}
}

test_dob_pattern {
	output.message == "DOB" with input as {"message":"11/30/2023"}
}

# dd/mm/yyyy

test_dob_pattern {
	output.message == "DOB" with input as {"message":"01/01/1900"}
}

test_dob_pattern {
	output.message == "DOB" with input as {"message":"1/1/1900"}
}

test_dob_pattern {
	output.message == "DOB" with input as {"message":"10/01/1971"}
}

test_dob_pattern {
	output.message == "DOB" with input as {"message":"30/11/2023"}
}


# yyyy/mm/dd

test_dob_pattern {
	output.message == "DOB" with input as {"message":"1900/01/10"}
}

test_dob_pattern {
	output.message == "DOB" with input as {"message":"1900/1/1"}
}

test_dob_pattern {
	output.message == "DOB" with input as {"message":"1971/10/01"}
}

test_dob_pattern {
	output.message == "DOB" with input as {"message":"2023/12/31"}
}

# yyyy-mm-dd

test_dob_pattern {
	output.message == "DOB" with input as {"message":"1900-01-10"}
}

test_dob_pattern {
	output.message == "DOB" with input as {"message":"1900-1-1"}
}

test_dob_pattern {
	output.message == "DOB" with input as {"message":"1971-10-01"}
}

test_dob_pattern {
	output.message == "DOB" with input as {"message":"2023-12-31"}
}
