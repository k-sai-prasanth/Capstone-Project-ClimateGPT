# ClimateGPT Project

## Overview
The ClimateGPT project integrates state-of-the-art AI models, such as LLAMA, with climate research to provide real-time insights and solutions to global climate concerns. By combining sophisticated analytics, robust data handling mechanisms, and modular APIs, ClimateGPT aims to support decision-making for a sustainable future.

## Key Features
- **Real-time Climate Insights**: Integration of live and historical datasets for monitoring and analysis.
- **Advanced AI Models**: Utilizes LLAMA models for generating predictions and answering climate-related queries.
- **Modular Architecture**: Designed with modular APIs and tools for easy development and scalability.
- **Custom Tools**: Bespoke tools for monitoring emissions, surface temperature changes, etc.

---

## Repository Structure
The repository is structured for ease of collaboration and modular development:

### **Files Structure**

### **Folders**
- **Datasets/**  
  Contains raw and processed datasets in formats like `.csv` and `.json` used throughout the project.

- **Src/**  
  Houses the source code and tools:
  - **Commons/**: Shared modules, utility functions, and configurations.
  - **Custom_tools/**: Project-specific tools including:
    - `emission_data_average.py`: Calculates average emission data.
    - `surface_temperature_change.py`: Monitors Earth's surface temperature changes.
    - `carbon_monitor.py`: Tracks carbon emissions.
  - **Static/**: Stores static resources such as images, templates, or configuration files.

- **Tests/**  
  Contains unit and integration test scripts to ensure the reliability of tools and application components.

### **Files**
- **app.py**  
  The central application file built with FastAPI, integrating all tools and APIs.

- **.coverage**  
  A coverage report file tracking test coverage using `coverage.py`.

- **.gitignore**  
  Lists files and directories to be ignored by Git (e.g., environment files, logs).

- **README.md**  
  Documentation for the project, including an overview, setup instructions, and usage guidelines.

- **requirements.txt**  
  Specifies the Python dependencies required to run the project.

---

## Installation and Setup

### Prerequisites
Ensure you have the following installed:
- Python 3.9+
- pip (Python package manager)

### Steps
1. Clone the repository:
   ```bash
   git clone https://github.com/newsconsole/GMU_DAEN_Team_3.git
   cd GMU_DAEN_Team_3
   ```
### Install dependencies:
```bash
pip install -r requirements.txt
``` 
### Configure the .env file with the necessary credentials.

## Usage
### Running the Application
1. Start the application:
   ```bash
   python src/app.py
   ```
3. Start the web app:
   ```bash
   uvicorn app:app --reload from src folder
   ```
5. Access the application through the FastAPI interface, typically at http://127.0.0.1:8000.

### Tools and Features

Below are the tools included in the ClimateGPT project, along with their descriptions:

1. **CIL_TAS_Tool**  
   Queries global temperature and sea-level projections to analyze climate trends and impacts using Climate Impact Lab datasets.

2. **RenNinjaTool**  
   Processes renewable energy datasets (PV and wind) to analyze energy trends, forecast capacity, and support renewable energy policy decisions.

3. **EmissionsAnalysisTool**  
   Analyzes greenhouse gas emissions across sectors and geographies for tracking, comparison, and trend analysis.

4. **WeatherDataTool**  
   Extracts and analyzes state-level weather data for historical climate insights and regional weather trends.

5. **EnergyMixTool**  
   Provides insights into energy production and consumption across various energy sources to evaluate transitions and dependencies.

6. **DeforestationDataTool**  
   Processes deforestation data to assess environmental impacts and track forest loss trends globally or regionally.

7. **ClimateIndicatorsTool**  
   Analyzes key climate indicators like temperature anomalies, sea-level rise, and extreme weather occurrences for comprehensive climate assessment.

8. **CountryRatingTool**  
   Aggregates and rates countries based on their environmental performance and sustainability metrics.

9. **SectorEmissionsTool**  
   Evaluates sector-specific emissions data to track industrial, transportation, and energy-related greenhouse gas outputs.

10. **CarbonMonitorTool**  
    Provides real-time monitoring and historical trends of global carbon emissions for immediate analysis and response.

11. **GlobalSurfaceWaterTool**  
    Analyzes surface water changes globally to understand hydrological impacts of climate change and human activities.

12. **FuelDataTool**  
    Processes fuel-related datasets to assess consumption, emissions, and energy efficiency across different energy types.

13. **GreenHouseEmissionsTool**  
    Tracks greenhouse gas emissions at a global scale, enabling comparative analysis of regions and policy impacts.

14. **EnergyEmissionsTool**  
    Combines energy consumption data with emissions metrics to assess the carbon intensity of various energy sources.

15. **SectorEmissionsCombinedTool**  
    Integrates multiple emissions datasets to provide a holistic view of sector-wide emissions trends and drivers.

This section can be added to the README file or documentation for a quick overview of the project tools. Let me know if you need further refinements!


## Testing
Run tests: 
```bash
pytest
```
Coverage reports can be generated using:
```bash
coverage run -m pytest
coverage report
```
## Future Enhancements
Expand the range of datasets to include financial, policy, and additional climate-related data.
Enhance the integration of the LLAMA model for improved insights.
Develop a user-friendly front-end interface.
Implement additional tools for renewable energy analysis and policy impact assessment.


## Contributors
Team Twisters - DEAN 690

## Acknowledgments
We want to thank George Mason University and Erasmus.AI for their guidance and support in developing this project.

