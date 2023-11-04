# CS441_Fall2023_HW2
## Swetha Gumpena
### UIN: 670605665
### NetID: sgumpe2@uic.edu

Repo for the Spark Homework-2 for CS441-Fall2023

---

AWS EMR Deployment video link: 

---

## Environment:
**OS** : Mac OS

---

## Prerequisites:
- SBT
- Spark Version 3.4.1
- Java 11

---

## Running the project
1) Download the repo from git
2) The root file is found in _src/main/scala/MainClass.scala_
3) Run `sbt clean compile` from the terminal
4) Run `sbt "run <input-path> <output-path>"` without the angular braces
5) Run `sbt test` to test
6) To create the jar file, run the command `sbt -mem 2048 assembly`
7) The resulting jar file can be found at _target/scala-2.13/graphs_mitm_attack.jar_
8) If you are running using IntelliJ, import the project into the IDE, build it and create a `configuration` for MainClass.scala. The arguments in this would be the input files (original graph, perturbed graph, yaml of differences generated by NetGameSim) and output folder separated by a space i.e., `<.ngs file of original graph> <.ngs file of perturbed graph> <yaml of differences> <output folder>`
- Configuration example - `inputs/NetGameSimNetGraph_29-10-23-16-16-17.ngs inputs/NetGameSimNetGraph_29-10-23-16-16-17.ngs.perturbed inputs/NetGameSimNetGraph_29-10-23-16-16-17.ngs.yaml src/main/scala/resources/output`

- Make sure that your local input/output folder has the requisite permissions to allow the program to read and write to it

---

### Parameters
1. Sample input path - ```inputs/NetGameSimNetGraph_29-10-23-16-16-17.ngs inputs/NetGameSimNetGraph_29-10-23-16-16-17.ngs.perturbed inputs/NetGameSimNetGraph_29-10-23-16-16-17.ngs.yaml```
2. Sample output path - ```src/main/scala/resources/output```

---

## Requirements:

In this homework, we have to .
1) 

Other Requirements:

1) 

---

## Technical Design

We will take a look at the detailed description of how each of these pieces of code work below. Line by line comments explaining every step are also added to the source code in this git repo:

1) ### [MainClass.scala](src/main/scala/MainClass.scala)
  
2) 
---

## Test Cases
These are run through the command `sbt test`

---
