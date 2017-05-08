package main

import "github.com/Sirupsen/logrus"

func debugFields(function, structure, package1 string) logrus.Fields {
	fields := logrus.Fields{
		"function": function,
		"package":  package1,
	}
	if structure != "" {
		fields["structure"] = structure
	}
	return fields
}
