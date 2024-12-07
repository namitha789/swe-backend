from typing import Union

from fastapi import FastAPI
import requests
from collections import defaultdict
import urllib.parse
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

GRAPH_DB_URL = "http://localhost:7200/repositories/EnvProject"

@app.get("/full-data")
def read_root():
    query = """
PREFIX ex: <http://www.semanticweb.org/rathors/ontologies/2024/10/untitled-ontology-11#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT ?state ?pm25value ?ozonevalue ?pollutantvalue ?co2emission ?statepopulation
WHERE {
    ?state a ex:State .

    ?state ex:hasPM2_5Days ?pmclass .
    ?pmclass ex:hasPersonDaysExceedingPM2_5StandardValue ?pm25value .
    
    ?state ex:hasOzoneDays ?ozoneclass .
    ?ozoneclass ex:hasDaysExceedingOzoneStandardValue ?ozonevalue .
    
    ?state ex:hasPollutandPounds ?pollutantclass .
    ?pollutantclass ex:hasPollutantsValue ?pollutantvalue .
    
    ?state ex:hasCO2Emission ?co2class .
    ?co2class ex:hasCO2EmissionValue ?co2emission .

    ?state ex:hasPopulation ?statepopulation .
}
""".strip()

    print("Generated SPARQL Query:\n", query)
    headers = {"Accept": "application/sparql-results+json"}

    # Define the query parameters
    params = {"query": query}

    # Make the GET request
    response = requests.get(GRAPH_DB_URL, headers=headers, params=params)

    # Handle the response
    if response.status_code == 200:
        print("Success!")
    else:
        print(f"Request failed with status code {response.status_code}: {response.text}")

    return parse(response.json())


@app.get("/pm25/{op}/{value}")
def pm25(op: str, value: int):
    # return {"op": op, "value": value}

    if op == "greater":
        op = ">"
    elif op == "lesser":
        op = "<"
    else:
        op = '='
    
    query = """
PREFIX ex: <http://www.semanticweb.org/rathors/ontologies/2024/10/untitled-ontology-11#>

PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT ?state ?pm25value ?statepopulation ?statearea ?statedivision ?stateregion
WHERE {
  ?state a ex:State .
  ?state ex:hasPopulation ?statepopulation .
  ?state ex:hasStateArea ?statearea .
  ?state ex:hasStateDivision ?statedivision .
  ?state ex:hasStateRegion ?stateregion .

  ?state ex:hasPM2_5Days ?pmclass .
  ?pmclass ex:hasPersonDaysExceedingPM2_5StandardValue ?pm25value .
  FILTER(?pm25value """ + op + " " + str(value) + """)
}
""".strip()

    print("Generated SPARQL Query: \n", query)

    headers = {"Accept": "application/sparql-results+json"}

    # Define the query parameters
    params = {"query": query}

    # Make the GET request
    response = requests.get(GRAPH_DB_URL, headers=headers, params=params)

    # Handle the response
    if response.status_code == 200:
        print("Success!")
    else:
        print(f"Request failed with status code {response.status_code}: {response.text}")

    res = parse(response.json())
    return res


@app.get("/ozone/{op}/{value}")
def ozone(op: str, value: int):

    if op == "greater":
        op = ">"
    elif op == "lesser":
        op = "<"
    else:
        op = '='
    
    query = """
PREFIX ex: <http://www.semanticweb.org/rathors/ontologies/2024/10/untitled-ontology-11#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT ?state ?ozonevalue ?statepopulation ?statearea ?statedivision ?stateregion
WHERE {
  ?state a ex:State .
  ?state ex:hasPopulation ?statepopulation .
  ?state ex:hasStateArea ?statearea .
  ?state ex:hasStateDivision ?statedivision .
  ?state ex:hasStateRegion ?stateregion .

  ?state ex:hasOzoneDays ?ozoneclass .
  ?ozoneclass ex:hasDaysExceedingOzoneStandardValue ?ozonevalue .
  FILTER(?ozonevalue """ + op + " " + str(value) + """)
}
""".strip()

    print("Generated SPARQL Query: \n", query)

    headers = {"Accept": "application/sparql-results+json"}

    # Define the query parameters
    params = {"query": query}

    # Make the GET request
    response = requests.get(GRAPH_DB_URL, headers=headers, params=params)

    # Handle the response
    if response.status_code == 200:
        print("Success!")
    else:
        print(f"Request failed with status code {response.status_code}: {response.text}")

    res = parse(response.json())
    return res


@app.get("/pollutants/{op}/{value}")
def pollutants(op: str, value: int):

    if op == "greater":
        op = ">"
    elif op == "lesser":
        op = "<"
    else:
        op = '='
    
    query = """
PREFIX ex: <http://www.semanticweb.org/rathors/ontologies/2024/10/untitled-ontology-11#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT ?state ?pollutantsvalue ?statepopulation ?statearea ?statedivision ?stateregion
WHERE {
  ?state a ex:State .
  ?state ex:hasPopulation ?statepopulation .
  ?state ex:hasStateArea ?statearea .
  ?state ex:hasStateDivision ?statedivision .
  ?state ex:hasStateRegion ?stateregion .

  ?state ex:hasPollutandPounds ?pollutantclass .
  ?pollutantclass ex:hasPollutantsValue ?pollutantsvalue .
  FILTER(?pollutantsvalue """ + op + " " + str(value) + """)
}
""".strip()

    print("Generated SPARQL Query: \n", query)

    headers = {"Accept": "application/sparql-results+json"}

    # Define the query parameters
    params = {"query": query}

    # Make the GET request
    response = requests.get(GRAPH_DB_URL, headers=headers, params=params)

    # Handle the response
    if response.status_code == 200:
        print("Success!")
    else:
        print(f"Request failed with status code {response.status_code}: {response.text}")

    res = parse(response.json())
    return res



@app.get("/co2/{op}/{value}")
def co2(op: str, value: int):

    if op == "greater":
        op = ">"
    elif op == "lesser":
        op = "<"
    else:
        op = '='
    
    query = """
PREFIX ex: <http://www.semanticweb.org/rathors/ontologies/2024/10/untitled-ontology-11#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT ?state ?co2emission ?statepopulation ?statearea ?statedivision ?stateregion
WHERE {
  ?state a ex:State .
  ?state ex:hasPopulation ?statepopulation .
  ?state ex:hasStateArea ?statearea .
  ?state ex:hasStateDivision ?statedivision .
  ?state ex:hasStateRegion ?stateregion .

  ?state ex:hasCO2Emission ?co2class .
  ?co2class ex:hasCO2EmissionValue ?co2emission .
  FILTER(?co2emission """ + op + " " + str(value) + """)
}
""".strip()

    print("Generated SPARQL Query: \n", query)

    headers = {"Accept": "application/sparql-results+json"}

    # Define the query parameters
    params = {"query": query}

    # Make the GET request
    response = requests.get(GRAPH_DB_URL, headers=headers, params=params)

    # Handle the response
    if response.status_code == 200:
        print("Success!")
    else:
        print(f"Request failed with status code {response.status_code}: {response.text}")

    res = parse(response.json())
    return res


def parse(res):
    d = defaultdict(list)
    keys = res["head"]["vars"]

    for binding in res["results"]["bindings"]:
        for key in keys:
            val = binding[key]['value']
            if key == 'state':
                # TODO: CHANGE
                val = urllib.parse.unquote(val.split('#')[-1])
                d[key].append(val)
            elif key in ('stateregion', 'statedivision'):
                d[key].append(val)
            elif key == 'co2emission':
                d[key].append(float(val))
            else:
                d[key].append(int(val))
    return d

