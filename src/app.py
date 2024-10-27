from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import json
import os
from groq import Groq
from emissions_data import EmissionDataTool  # Specific year data
from emission_data_average import EmissionDataTool_Average  # Average data
from surface_temperature_change import SurfaceTemperatureChangeTool  # Earth Surface Temperature Change Data
from carbon_monitor import CarbonEmissionDataTool # Carbon Emissions Data

# Initialize the FastAPI app
app = FastAPI()

# Mount static files for frontend
static_path = os.path.join(os.path.dirname(__file__), 'static')
app.mount("/static", StaticFiles(directory=static_path), name="static")

# Serve the index.html at the root
@app.get("/")
async def serve_frontend():
    return FileResponse(static_path + "/index.html")

# Set the Groq API key for the LLaMA model
GROQ_API_KEY = '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'

# Declare the Groq API client
groq_client = Groq(api_key=GROQ_API_KEY)

# LLaMA Model Versions
llama_70B_tool_use = 'llama3-groq-70b-8192-tool-use-preview'

# User Query Handling Class
class UserQueryHandler:
    def __init__(self):
        self.tool_specific_emission = EmissionDataTool()
        self.tool_average_emission = EmissionDataTool_Average()
        self.tool_earth_surface_temperature_change = SurfaceTemperatureChangeTool()
        self.tool_carbon_emissions = CarbonEmissionDataTool()
        #Declare any new tools above this line

    # Function to handle queries
    async def fetch_response_userquery(self, function_name: str, parameters: dict):
        # Dynamically call the respective tool based on the function name
        if function_name == "get_emission_data":
            return await self.tool_specific_emission.run_impl(**parameters)
        elif function_name == "get_average_emission_data":
            return await self.tool_average_emission.run_impl(**parameters)
        elif function_name == "get_surface_temperature_change":
            return await self.tool_earth_surface_temperature_change.run_impl(**parameters)
        elif function_name == "get_carbon_emission_data":
            return await self.tool_carbon_emissions.run_impl(**parameters)
        #Declare any new Tools above this line
        else:
            return {"status": "error", "data": ["Unknown function requested by the model."]}

# Tool declaration in the form of a JSON block
def get_tool_declaration():
    return """
    <tools> 
    {
        "name": "get_emission_data",
        "description": "Retrieve emission values for a specified country (or list of countries), year (or list of years), and emission type (or list of emission types). The function will return a JSON object with the requested information. If multiple countries, emission types, or specific years are queried, the function will provide the corresponding outputs for each. If the requested emission type does not exist, the function will return all emission types for the given country and year and suggest likely emission types. When parsing the parameters, if terms like 'sfc', 'methane', 'pfc', 'nf3', 'n2o', 'HFC PFC', or 'greenhouse' are mentioned, convert them to their respective emission types: 'sfc_emissions', 'methane_emissions', 'pfc_emissions', 'nf3_emissions', 'n2o_emissions', 'HFC_PFC_emissions', or 'green_house_emissions'. If the term is mentioned with or without the 'emissions' keyword, ensure it's mapped correctly for function calling. This tool accepts multiple countries, years, and emission types as lists. ",
        "parameters": {
            "properties": {
                "country": {
                    "description": "The name of the country or a list of countries.",
                    "type": "array",
                    "items": {
                        "type": "string"
                    }
                    "description": "The name of the country or a list of countries.",
                    "type": "array",
                    "items": {
                        "type": "string"
                    }
                },
                "year": {
                    "description": "The year or a list of years for which the emission data is requested.",
                    "type": "array",
                    "items": {
                        "type": "integer"
                    }
                    "description": "The year or a list of years for which the emission data is requested.",
                    "type": "array",
                    "items": {
                        "type": "integer"
                    }
                },
                "emission_type": {
                    "description": "The emission type or a list of emission types. If the requested type doesn't exist, all emission types will be returned.",
                    "type": "array",
                    "items": {
                        "type": "string"
                    }
                }
                    "description": "The emission type or a list of emission types. If the requested type doesn't exist, all emission types will be returned.",
                    "type": "array",
                    "items": {
                        "type": "string"
                    }
                }
            }
        }
    },
    {
        "name": "get_average_emission_data",
        "description": "Get the average emission value for a country across all years for a specified emission type, or the trend for the first/last x years. If terms like ‘decade’ or similar are used, convert them into the corresponding number of years.",
        "parameters": {
            "properties": {
                "country": {
                    "description": "The name of the country or area.",
                    "type": "string"
                },
                "emission_type": {
                    "description": "The type of emission (e.g., 'sfc_emissions', 'n2o_emissions', 'methane_emissions, green_house_emissions, etc').",
                    "type": "string"
                },
                "trend_type": {
                    "description": "Defines whether to get 'average', 'last x years', 'first x years', or 'trend for x years'.",
                    "type": "string",
                    "enum": ["average", "last x years", "first x years", "trend for x years"],
                    "default": "average"
                },
                "num_years": {
                    "description": "The number of years to use for trends (applicable only when using 'last x years', 'first x years', or 'trend for x years').",
                    "type": "integer",
                    "default": 5
                "description": "The type of emission (e.g., 'sfc_emissions', 'n2o_emissions', 'methane_emissions, green_house_emissions, etc').",
                "type": "string"
                },
                "trend_type": {
                    "description": "Defines whether to get 'average', 'last x years', 'first x years', or 'trend for x years'.",
                    "type": "string",
                    "enum": ["average", "last x years", "first x years", "trend for x years"],
                    "default": "average"
                },
                "num_years": {
                    "description": "The number of years to use for trends (applicable only when using 'last x years', 'first x years', or 'trend for x years').",
                    "type": "integer",
                    "default": 5
                }
            }
        }
    },
    {
        "name": "get_surface_temperature_change",
        "description": "This tool allows the user to query earth's surface temperature change data for one or more countries for a particular year or range of years or for decades," 
                       "It supports multiple operations, such as returning earth's surface temperature change for a particualr country, comparison between countries,"
                       "finding top 'n' countries with highest or lowest temperature changes, retrieving data that crosses a certain threshold, analyzing data over decades",
                       "The data is available from year 1961 to year 2023",
        "parameters": {
            "properties": {
                "command": {
                    "description": "It has list of commands to chose based on the question type.The available commands are temperature_change_for_country, temperature_change_between_years, compare_temperature_change," 
                                    "top_n_temperature_change, threshold_exceeded.For example if question is regarding the surface temperature change for a particular country, then command = temperature_change_for_country.",
                    "type": "string"
                },
                "country": {
                    "description": "The country or list of countries for which to fetch temperature change data."
                                    "For example, 'India' or 'India, Brazil'. If not provided, data for all countries will be aggregated.",
                    "type": "string"
                },
                "start_year": {
                    "description: "The starting year for the data. If not provided, it will include all available years.",
                    "type": "int"
                },
                "end_year": {
                    "description: "The ending year for the data. If not provided, it will include all available years.",
                    "type": "int"
                },
                "threshold": {
                    "description: "Temperature change threshold in degrees Celsius. Used to filter results",
                    "type": "float"
                },
                "top_n": {
                    "description: "The number of top countries or regions to retrieve based on temperature change. Default is 5.",
                    "type": "int"
                },
                "ascending": {
                    "description: "If True, returns countries or regions with the lowest anomalies, else returns the highest.",
                    "type": "bool"
                },
                "decade_start": {
                    "description: "The starting year of the decade for decade-level analysis. Should be a multiple of 10.",
                    "type": "int"
                },
                "interval": {
                        "description": "The interval or shift in years. Used to get data at every 'interval' years starting from 'start_year'.",
                        "type": "int"
                },
            }
        }
    },
    {
        "name": "get_carbon_emission_data",
        "description": "This tools helps to query CO2 emissions also known as carbon emissions data by specifying countries, sectors, years, and/or dates. "
                        "If any of the requested parameters are not available, it provides relevant messages."
                        "It has information for every day from January to July for the years 2023 and 2024."
                        "If the data or year requested is not in this range, provide the value for the closest date to the requested data."
                        "The information is specified for the following countries only: Brazil, China, European Union, France, Germany, India, Italy, Japan, Russia, Spain, United Kingdom, United States, Rest of the World and WORLD."
                        "It has the data for the following sectors, Domestic Aviation, Ground Transport, Industry, Residential, Power and ,International Aviation."
        "parameters": {
            "properties": {
                "countries": {
                    "description": "The country or list of countries for which to fetch the co2 or carbon emissions data. "
                                    "The information is specified for the following countries only: Brazil, China, European Union, France, Germany, India, Italy, Japan, Russia, Spain, United Kingdom, United States, Rest of the World and WORLD."
                                    "For example, 'India' or 'India, Brazil'. If not provided, data for all countries will be aggregated.",
                    "type": "List[str]"
                },
                "sectors": {
                    "description": "The sector or list of sectors for which to fetch the co2 or carbon emissions data. "
                                    "It has the data for the following sectors, Domestic Aviation, Ground Transport, Industry, Residential, Power and ,International Aviation"
                                    "For example, 'Residential' or 'Residential, Power'. If not provided, data for all sectors will be aggregated.",
                    "type": "List[str]"
                },
                "years": {
                    "description": "The specific year or list of years for which to filter the co2 or carbon emissions data. "
                                    "It has information for every day from January to July for the years 2023 and 2024."
                                    "For example, '2023' or '2023, 2024'.",
                    "type": "List[int]",
                },
                "dates": {
                    "description": "The specific date for which to filter the co2 or carbon emissions data. "
                                    "The data is in the format of DD/MM/YYYY "
                                    "It has information for every day from January to July for the years 2023 and 2024."
                                    "For example, '01/01/2023'. ",
                    "type": "List[str]",
                },
            }
        }
    }
    </tools>
    """

# API Endpoint to process user questions
class UserQuestion(BaseModel):
    question: str

# Dictionary to store session memory
memory_store = {}

# Function to update memory for a session
def update_memory(session_id, user_input, model_response):
    if session_id not in memory_store:
        memory_store[session_id] = []
    memory_store[session_id].append({"user": user_input, "model": model_response})

# Function to retrieve memory for a session
def get_memory(session_id):
    return memory_store.get(session_id, [])

@app.post("/ask")
async def ask_question(user_question: UserQuestion):
    session_id = 1
    question = user_question.question
    tool_declaration = get_tool_declaration()

    #Testing
    print("Original Question:", question)

    # Step 1: Pre-process the question to handle human language phrases
    clarification_prompt = (
        f"The user asked: '{question}'. "
        "If the user's question contains informal or ambiguous phrases like 'kangaroo country', "
        "translate them into a formal or precise name. Return the clarified input."
    )

    clarification_response = groq_client.chat.completions.create(
        messages=[
            {"role": "system", "content": clarification_prompt},
            {"role": "user", "content": question},
        ],
        model=llama_70B_tool_use,
    )

    # Get the clarified input from the model
    clarified_question = clarification_response.choices[0].message.content.strip()

    #Testing
    print("Clarified Question:", clarified_question)

    # Check if clarification failed and ask for more details if necessary
    if "I don't understand" in clarified_question:
        return JSONResponse(content={"response": "I'm not sure I understood your question. Could you clarify or provide more detail?"})

    # Step 2: Get memory for this session (optional if you want memory)
    memory = get_memory(session_id)

    # Format memory for LLaMA
    memory_context = "\n".join([f"User: {entry['user']}\nModel: {entry['model']}" for entry in memory])

    # Step 3: Query the LLaMA model with clarified input
    decision_prompt = (
        f"Memory:\n{memory_context}\n\n"
        f"The user asked: '{clarified_question}'. "
        f"You are a top-notch climate assistant. You should be able to handle user queries and respond accordingly. "
        f"You have access to the following tools: {tool_declaration}. "
        f"Use these tools to answer the user queries when appropriate. "
        f"If the user's question is casual or conversational and dont need any tools, generate a friendly, natural response. "
        f"For each user query you can answer using the tools, understand the question carefully and return a JSON object with the function name and the arguments required, enclosed within <tool_call> and </tool_call> tags. "
        f"If the user's question does not relate to any of the tools, or if you cannot identify a tool to use, respond to the user directly and engagingly like an assistant would. "
        f"Consider the meaning and context of different user queries, even if phrased differently or with any spelling mistakes, and map them to the appropriate tool when possible."
        f"Understand and decode differnet types of shortforms, normal idioms, phrases, abbreviations etc. For example, ind stands for india"
        f"If the user question is in high level language, translate it to the normal language, convert any phrases,short forms or abbreviations and understand it and then decide on the tool and Json object."
        f"For example, if the user asks 'Give me the surface temperature change for India from 1970 in a 5-year shift', you should return: "
        f"name: get_surface_temperature_change, arguments: {{'command': 'temperature_change_for_country', 'country': 'India', 'start_year': 1970, 'interval': 5}} ."
    )

    # LLaMA response to decide between tool invocation and casual conversation
    decision_response = groq_client.chat.completions.create(
        messages=[
            {"role": "system", "content": decision_prompt},
            {"role": "user", "content": clarified_question},
        ],
        model=llama_70B_tool_use,
    )

    llama_response = decision_response.choices[0].message.content

    function_and_parameters = extract_function_and_parameters(llama_response)

    #Testing
    print("Function Call Triggered:",function_and_parameters)

    if function_and_parameters:
        # Extract function name and parameters
        function_name = function_and_parameters.get("function_name")
        parameters = function_and_parameters.get("arguments")

        if function_name and parameters:
            # Call the appropriate tool using UserQueryHandler
            query_handler = UserQueryHandler()
            response_data = await query_handler.fetch_response_userquery(
                function_name=function_name,
                parameters=parameters
            )
            #Testing
            print("Function Returned Data:",response_data)
            # Use LLaMA model to generate a descriptive response based on the user query and data
            detailed_response_prompt = (
                f"The user asked: '{question}'. The data retrieved is: '{response_data}'. "
                f"Please generate a user friendly and a short ans simple descriptive response that clearly explains the result to the user. "
                f"If needed, perform any additional analysis on the data before providing the response. Do not mention any internal analysis steps."
                f"If receieved an error message in the response data, give response without revealing the error or talking about the error in your response. Give a generic response."
                f"Try to give the responses in a more easy and readable format by using several things like bullet points."
            )

            response_generation = groq_client.chat.completions.create(
                messages=[
                    {"role": "user", "content": detailed_response_prompt}
                ],
                model=llama_70B_tool_use,
            )

            descriptive_response = response_generation.choices[0].message.content
            # Update memory after the response (if using memory)
            update_memory(session_id, question, descriptive_response)

            #print("Model memory: ", memory_store.get(1, []))
            return JSONResponse(content={"response": descriptive_response})

    # If no function name and parameters were extracted, have the LLM generate a response directly
    else:
        # Provide the user query to the LLM and ask it to respond appropriately
        chat_completion = groq_client.chat.completions.create(
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a top-notch climate assistant. Answer the user's question directly and engagingly. Generate afriendly and natural response."
                    )
                },
                {"role": "user", "content": question},
            ],
            model=llama_70B_tool_use,
        )

        llama_response_general = chat_completion.choices[0].message.content
        # Update memory after the conversational response
        update_memory(session_id, question, llama_response_general)

        #print("Model memory: ", memory_store.get(1, []))
        return JSONResponse(content={"response": llama_response_general})

# Function to parse the LLaMA model response and extract the function and parameters
def extract_function_and_parameters(response: str):
    try:
        tool_call_start = response.find("<tool_call>")
        tool_call_end = response.find("</tool_call>")
        if tool_call_start == -1 or tool_call_end == -1:
            return None

        tool_call_json = response[tool_call_start + len("<tool_call>"):tool_call_end]
        tool_call_data = json.loads(tool_call_json)

        return {
            "function_name": tool_call_data.get("name"),
            "arguments": tool_call_data.get("arguments"),
        }
    except Exception as e:
        return None
