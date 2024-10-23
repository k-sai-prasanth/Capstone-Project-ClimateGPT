from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import asyncio
import json
import requests
import os
from groq import Groq
from emissions_data import EmissionDataTool  # Specific year data
from emission_data_average import EmissionDataTool_Average  # Average data

# Initialize the FastAPI app
app = FastAPI()
OPENWEATHER_API_KEY = '58be21383da9a93bdc2f76ed9d984acd'

# Mount static files for frontend
static_path = os.path.join(os.path.dirname(__file__), 'static')
app.mount("/static", StaticFiles(directory=static_path), name="static")

# Serve the index.html at the root
@app.get("/")
async def serve_frontend():
    return FileResponse(static_path + "/index.html")

# Set the Groq API key for the LLaMA model
GROQ_API_KEY = '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'

# Declaring the Groq API client
groq_client = Groq(api_key=GROQ_API_KEY)

# LLaMA Model Versions
llama_70B_tool_use = 'llama3-groq-70b-8192-tool-use-preview'
llama_70B_response_generation = 'llama3-groq-70b-8192-tool-use-preview'

# Emission Query Handling Class
class EmissionQueryHandler:
    def __init__(self):
        self.tool_specific = EmissionDataTool()  # Tool for specific year data
        self.tool_average = EmissionDataTool_Average()  # Tool for average data

    # Function to handle both specific year and average queries
    async def fetch_emission_data(self, function_name: str, parameters: dict):
        # Dynamically call the respective tool based on the function name
        if function_name == "get_emission_data":
            return await self.tool_specific.run_impl(**parameters)
        elif function_name == "get_average_emission_data":
            return await self.tool_average.run_impl(**parameters)
        else:
            return "Unknown function requested by the model."

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
                },
                "year": {
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
            },
            "required": ["country"],
            "type": "object"
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
                }
            },
            "required": ["country", "emission_type"],
            "type": "object"
        }
    }

    </tools>
    """

# API Endpoint to process user questions
class UserQuestion(BaseModel):
    question: str

@app.post("/ask")
async def ask_question(user_question: UserQuestion):
    question = user_question.question
    tool_declaration = get_tool_declaration()

    # Query the LLaMA model (Groq API) to determine which tool to use
    chat_completion = groq_client.chat.completions.create(
        messages=[
            {
                "role": "system",
                "content": (
                    f"You are a function-calling AI model. You have access to the following tools: {tool_declaration}. "
                    "For each function call, return a JSON object with the function name and the required arguments. The arguments can include multiple values, such as lists for a single field (e.g., multiple years or countries), based on the context of the question asked." 
                    "Please consider the meaning and context of different user queries, even if they are phrased differently, and map them to the appropriate tool accordingly."
                )
            },
            {"role": "user", "content": question},
        ],
        model=llama_70B_tool_use,
    )

    llama_response = chat_completion.choices[0].message.content
    function_and_parameters = extract_function_and_parameters(llama_response)
    print(function_and_parameters)
    if function_and_parameters:
        # Extract function name and parameters
        function_name = function_and_parameters.get("function_name")
        parameters = function_and_parameters.get("arguments")

        if function_name and parameters:
            # Call the appropriate emission tool using EmissionQueryHandler
            query_handler = EmissionQueryHandler()
            emission_data = await query_handler.fetch_emission_data(
                function_name=function_name,
                parameters=parameters
            )

            # Use LLaMA model to generate a descriptive response based on the user query and emission data
            detailed_response_prompt = (
                f"The user asked: '{question}'. The emission data retrieved is: '{emission_data}'. "
                f"Please analyze the question and the retrieved emission data. Based on the question, perform any necessary data analysis, or handle specific requests. Use the provided emission data to generate insights, perform calculations if required, and deliver an accurate response. Return the output in a clear and descriptive manner, ensuring that the answer directly addresses the question asked"
            )

            response_generation = groq_client.chat.completions.create(
                messages=[
                    {"role": "user", "content": detailed_response_prompt}
                ],
                model=llama_70B_response_generation,
            )

            descriptive_response = response_generation.choices[0].message.content
            return JSONResponse(content={"response": descriptive_response})

    return JSONResponse(content={"response": "Could not extract the function name or parameters."})

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

class WeatherQueryHandler:
    def __init__(self):
        self.api_key = OPENWEATHER_API_KEY
        self.base_url = 'http://api.openweathermap.org/data/2.5/weather'

    def fetch_weather_data(self, city: str):
        """Fetch current weather data for a given city."""
        params = {
            'q': city,
            'appid': self.api_key,
            'units': 'metric'
        }
        response = requests.get(self.base_url, params=params)
        
        if response.status_code == 200:
            return response.json()
        else:
            return {"error": f"Could not retrieve weather data for {city}"}

class UserQuestion(BaseModel):
    question: str

@app.get("/")
def read_root():
    return {"message": "Welcome to the ClimateGPT API. Use the '/ask_weather' endpoint to query weather information."}

def extract_city_from_question(question: str):
    """A simple function to extract city name from the question."""
    if "in" in question:
        return question.split("in")[-1].strip().split()[0]
    return None

def simulated_model_response(question):
    if "weather" in question.lower():
        return """
        <tool_call>{
            "name": "get_weather_data",
            "arguments": {
                "city": "Paris"
            }
        }</tool_call>
        """
    return """
    <tool_call>{
        "name": "unknown_function",
        "arguments": {}
    }</tool_call>
    """

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

@app.post("/ask_weather")
def ask_weather_question(user_question: UserQuestion):
    question = user_question.question
    llama_response = simulated_model_response(question)
    function_and_parameters = extract_function_and_parameters(llama_response)
    
    if function_and_parameters:
        function_name = function_and_parameters.get("function_name")
        parameters = function_and_parameters.get("arguments")
        city = parameters.get("city")

        if function_name == "get_weather_data" and city:
            weather_handler = WeatherQueryHandler()
            weather_data = weather_handler.fetch_weather_data(city=city)

            if "error" not in weather_data:
                return JSONResponse(content={
                    "response": f"The weather in {city} is {weather_data['weather'][0]['description']}. "
                                f"The temperature is {weather_data['main']['temp']}°C, "
                                f"with a humidity of {weather_data['main']['humidity']}%."
                })
            else:
                return JSONResponse(content={"response": weather_data["error"]})

    return JSONResponse(content={"response": "Could not extract the function name or parameters."})

