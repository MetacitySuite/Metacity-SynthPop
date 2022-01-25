#import pandas as pd
import numpy as np

def configure(context):
    ...

def execute(context):
    #travel-survey
    travel_survey_values_dict = {}
    census_values_dict = {}
 
    travel_survey_values_dict['purpose_list_cz'] = ['bydliště', 'práce', 'vzdělávání', 'nakupování', 'volno', 'ostatní', 'zařizování', 'prac. cesta', 'stravování']
    travel_survey_values_dict['purpose_list'] = ['home', 'work', 'education', 'shop', 'leisure', 'other', 'other', 'other', 'shop']
    
    travel_survey_values_dict['mode_list_cz'] = ['auto-d', 'auto-p', 'bus', 'kolo', 'MHD', 'ostatní', 'pěšky', 'vlak']
    travel_survey_values_dict['mode_list'] = ['car', 'ride', 'pt', 'bike', 'pt', 'other', 'walk', 'pt']

    travel_survey_values_dict['employed_list_cz'] = ['zaměstnanec, zaměstnavatel, samostatně činný či pomáhající', 'pracující důchodce']
    travel_survey_values_dict['employed_list'] = ['employee, employer, self-employed, or helping', 'working retiree']
    
    travel_survey_values_dict['edu_list_cz'] = ['žák ZŠ','student SŠ', 'student VŠ','pracující SŠ student nebo učeň', 'pracující VŠ student']
    travel_survey_values_dict['edu_list'] = ['pupils, students, apprentices', 'pupils, students, apprentices', 'pupils, students, apprentices', 
                                                    'working students and apprentices', 'working students and apprentices']
    
    travel_survey_values_dict['unemployed_list_cz'] = ['nepracující důchodce', 'nezaměstnaný hledající první zaměstnání', 
                                                            'osoba s vlastním zdrojem obživy, na rodičovské dovolené', 
                                                            'osoba v domácnosti, dítě předškolního věku, ostatní závislé osoby',
                                                            'ostatní nezaměstnaní', 'žena na mateřské dovolené']
    travel_survey_values_dict['unemployed_list'] = ['non-working retiree', 'unemployed seeking first employment',
                                                        'with own source of living', 'person in household, pre-school child, other dependents',
                                                        'other unemployed', 'maternity leave']

    #census
    census_values_dict['employment_values'] = [1, 2, 3, 4, 6, 7, 8, 11, 12, 13]
    census_values_dict['employment_list'] = ['employee, employer, self-employed, or helping', 'working retiree', 
                        'working students and apprentices', 'maternity leave', 
                        'non-working retiree', 'with own source of living', 'pupils, students, apprentices', 
                        'unemployed seeking first employment', 'other unemployed', 'person in household, pre-school child, other dependents']
    
    return census_values_dict, travel_survey_values_dict