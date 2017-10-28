/**
 * @author Ting Pan <tpan35@gatech.edu>.
 */

package edu.gatech.cse8803.graphconstruct

import edu.gatech.cse8803.model._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


object GraphLoader {
  /** Generate Bipartite Graph using RDDs
    *
    * @input: RDDs for Patient, LabResult, Medication, and Diagnostic
    * @return: Constructed Graph
    *
    * */
  def load(patients: RDD[PatientProperty], labResults: RDD[LabResult],
           medications: RDD[Medication], diagnostics: RDD[Diagnostic]): Graph[VertexProperty, EdgeProperty] = {

   

    val sc = patients.sparkContext

    // get most recent diagnostic, medication, labredult events
    val diagDeduped = diagnostics.map(d=>((d.patientID, d.icd9code),d)).reduceByKey((d1,d2)=> if (d1.date>d2.date) d1 else d2).map{case(key, d)=>d}
    val medDeduped = medications.map(m=>((m.patientID, m.medicine),m)).reduceByKey((m1,m2)=> if(m1.date>m2.date) m1 else m2).map{case(key, m)=>m}
    val labDeduped = labResults.map(l=>((l.patientID,l.labName),l)).reduceByKey((l1,l2)=> if (l1.date>l2.date) l1 else l2).map{case(key, l)=>l}

    /**val patientVertexIdRDD = patients.map(p=>(p.patientID.toLong,p))
    val patient2VertexId = patientVertexIdRDD.collect.toMap
    val vertexPatient = patientVertexIdRDD.asInstanceOf[RDD[(VertexId, VertexProperty)]] */

    val vertexPatient: RDD[(VertexId, VertexProperty)] = patients.map(patient => (patient.patientID.toLong, patient.asInstanceOf[VertexProperty]))

    val startIndexDiag = patients.count()
    val diagVertexIdRDD = diagDeduped.map(_.icd9code).distinct().zipWithIndex().map{case(icd9code, zeroBasedIndex)=>(icd9code,zeroBasedIndex+1001)}
    val diag2VertexID = diagVertexIdRDD.collect.toMap
    val vertexDiagnostic = diagVertexIdRDD.map{case(icd9code,index )=>(index, DiagnosticProperty(icd9code))}.asInstanceOf[RDD[(VertexId,VertexProperty)]]

    val startIndexMed = diag2VertexID.size
    val medVertexIdRDD = medDeduped.map(_.medicine).distinct().zipWithIndex().map{case(medicine, zeroBasedIndex)=>(medicine, zeroBasedIndex+1001+startIndexMed)}
    val med2VertexID = medVertexIdRDD.collect.toMap
    val vertexMedication = medVertexIdRDD.map{case(medicine,index)=>(index, MedicationProperty(medicine))}.asInstanceOf[RDD[(VertexId,VertexProperty)]]

    val startIndexLab = med2VertexID.size
    val labVertexIdRDD = labDeduped.map(_.labName).distinct().zipWithIndex().map{case(labname, zeroBasedIndex)=>(labname, zeroBasedIndex+1001+startIndexMed+startIndexLab)}
    val lab2VertexID = labVertexIdRDD.collect.toMap
    val vertexLabResult = labVertexIdRDD.map{case(labname, index)=> (index, LabResultProperty(labname))}.asInstanceOf[RDD[(VertexId, VertexProperty)]]


    case class PatientPatientEdgeProperty(someProperty: SampleEdgeProperty) extends EdgeProperty

    val edgePatientPatient: RDD[Edge[EdgeProperty]] = patients
      .map({p =>
        Edge(p.patientID.toLong, p.patientID.toLong, SampleEdgeProperty("sample").asInstanceOf[EdgeProperty])
      })


    //construct edges

    val bcDiagnostic2VertexId = sc.broadcast((diag2VertexID))
    val edgePatientDiag = diagDeduped.map(d=>(d.patientID,d.icd9code,d)).map{case(patientID, icd9code, d)=>Edge(patientID.toLong,bcDiagnostic2VertexId.value(icd9code),PatientDiagnosticEdgeProperty(d).asInstanceOf[EdgeProperty])}
    val edgeDiagPatient = diagDeduped.map(d=>(d.patientID,d.icd9code,d)).map{case(patientID, icd9code, d)=>Edge(bcDiagnostic2VertexId.value(icd9code),patientID.toLong,PatientDiagnosticEdgeProperty(d).asInstanceOf[EdgeProperty])}

    val bcMedication2VertexId = sc.broadcast((med2VertexID))
    val edgePatientMed = medDeduped.map(m=>(m.patientID,m.medicine,m)).map{case(patientID, medicine, m)=>Edge(patientID.toLong,bcMedication2VertexId.value(medicine),PatientMedicationEdgeProperty(m).asInstanceOf[EdgeProperty])}
    val edgeMedPatient = medDeduped.map(m=>(m.patientID,m.medicine,m)).map{case(patientID, medicine, m)=>Edge(bcMedication2VertexId.value(medicine),patientID.toLong,PatientMedicationEdgeProperty(m).asInstanceOf[EdgeProperty])}

    val bcLabResult2VertexId = sc.broadcast((lab2VertexID))
    val edgePatientLab = labDeduped.map(l=>(l.patientID,l.labName,l)).map{case(patientID, labName, l)=>Edge(patientID.toLong,bcLabResult2VertexId.value(labName),PatientLabEdgeProperty(l).asInstanceOf[EdgeProperty])}
    val edgeLabPatient = labDeduped.map(l=>(l.patientID,l.labName,l)).map{case(patientID, labName, l)=>Edge(bcLabResult2VertexId.value(labName),patientID.toLong,PatientLabEdgeProperty(l).asInstanceOf[EdgeProperty])}

    // Making Graph

    val vertex = sc.union(vertexPatient,vertexDiagnostic,vertexMedication,vertexLabResult)
    val edgePD = sc.union(edgePatientDiag,edgeDiagPatient)
    val edgePM = sc.union(edgePatientMed,edgeMedPatient)
    val edgePL = sc.union(edgePatientLab,edgeLabPatient)
    val edge = sc.union(edgePD,edgePM,edgePL)

    val graph: Graph[VertexProperty, EdgeProperty] = Graph(vertex, edge)
    //println("graph: ")
    //println(graph.vertices.count())
    //println(graph.edges.count())

    graph
  }
}
