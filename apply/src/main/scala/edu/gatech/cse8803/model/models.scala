/**
  * @author Ting Pan <tpan35@gatech.edu>.
  */

package edu.gatech.cse8803.model

case class LabResult(patientID: String, date: Long, labName: String, value: String)

case class Diagnostic(patientID: String, date: Long, icd9code: String, sequence: Int)

case class Medication(patientID: String, date: Long, medicine: String)

abstract class VertexProperty

case class PatientProperty(patientID: String, sex: String, dob: String, dod: String) extends VertexProperty

case class LabResultProperty(testName: String) extends VertexProperty

case class DiagnosticProperty(icd9code: String) extends VertexProperty

case class MedicationProperty(medicine: String) extends VertexProperty

abstract class EdgeProperty

case class SampleEdgeProperty(name: String = "Sample") extends EdgeProperty

case class PatientLabEdgeProperty(labResult: LabResult) extends EdgeProperty

case class PatientDiagnosticEdgeProperty(diagnostic: Diagnostic) extends EdgeProperty

case class PatientMedicationEdgeProperty(medication: Medication) extends EdgeProperty

