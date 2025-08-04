# Portfolio: Data Engineering related projects
## 1. Project Airflow
This is an individual project for Apache Airflow based on the tutorial: 
https://youtu.be/3xyoM28B40Y?feature=shared

It has the following structure:


## 2. MSc Thesis Project: Inequality Join
The following structure shows the overall ...
Thesis_Ineuqality_Join
├───Master_Thesis.pdf: The master's thesis including summary, conclusion, etc.
├───Project_Airflow
└───Thesis_Inequality_Join
    ├───Evaluation: Jupyter notebooks to evaluate the efficiency of each inequality join algorithms (brute force, interval-based filtering, index-based filtering) based on the size of the data
    │   └───src: stores the python scripts for the generation of the data and the join algorithms
    └───Inequality_Join_Algo: Python scripts for the baseline algorithm (brute force), IEJoin (either interval based or index-based filtering) & RMJ/OMJ (Forward scan and backward scan)