Backend Code for SWE Project - Team 16

The backend is written in FastAPI framework in python.

It has REST endpoints that accept input parameters from the frontend UI and generate SPARQL queries from the input parameters and then relay them to the locally running GraphDB server.

The main.py file contains all the backend code.

To run it, install FastAPI and Uvicorn and run: uvicorn main:app --reload 
