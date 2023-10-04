class CONSTANTS:
    APP_TITLE = 'MM-Technologies Tools-1'
    TAB1_TITLE = "Пульсационные"
    TAB2_TITLE = "Пиковые"
    TAB3_TITLE = "Минимальные"
    
    AREA_TYPES = {
        'A': [0.15, 1.00, 0.76],  # default
        'B': [0.20, 0.65, 1.06],
        'C': [0.25, 0.40, 1.78],
    }
    WIND_AREA = {
        '1a': '170',
        '1 (Мск)': '230',  # default
        '2 (СПб)': '300',
        '3': '380',
        '4': '480',
        '5': '600',
        '6': '730',
        '7': '850',
        'Другой': '',
    }
    COEF_SPATIAL_CORR_PULS = '0.85'  # default
    COEF_SPATIAL_CORR_PIK = '1'  # default
    BUILDING_FREQUENCY = {
        'Нет': 0,
        'Да': 1,
    }
    DECREMENT = ['0.3', '0.15']
