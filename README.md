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

Capstone-Project-ClimateGPT/
├── src/                     # Source code
│   ├── app.py               # Main application file
│   ├── commons/             # Shared utilities and configurations
│   ├── custom_tools/        # Modular tools for querying specific datasets
│   ├── Tests/               # Unit tests for functionality verification
├── Datasets/                # Datasets integrated into the project
├── requirements.txt         # Python dependencies
├── README.md                # Project documentation
├── .env                     # Environment configuration
└── .gitignore               # Git ignored files

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


### Install dependencies:
pip install -r requirements.txt

## Usage
### Running the Application
1. Start the application: python src/app.py
2. Start the web app: uvicorn app:app --reload from src folder
3. Access the application through the FastAPI interface, typically at http://127.0.0.1:8000.

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
Run tests: pytest
Coverage reports can be generated using:
coverage run -m pytest
coverage report

## Future Enhancements
Expand the range of datasets to include financial, policy, and additional climate-related data.
Enhance the integration of the LLAMA model for improved insights.
Develop a user-friendly front-end interface.
Implement additional tools for renewable energy analysis and policy impact assessment.


## Contributors
Team Twisters - DEAN 690

## Acknowledgments
We want to thank George Mason University and Erasmus.AI for their guidance and support in developing this project.

