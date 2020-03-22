'''
Module with functions to calculate the hemodinamic variables.
Formulas are the ones that Dr. Dagnovar Aristizabal Ocampo uses.
'''

import math

def body_mass_index(weight, height):
    '''
    Calculates body mass index given in kg/m2
    Parameters
    ----------
    weight : given in kg
    height : given in meters
    '''
    return weight / (height ** 2)

def body_surface_area(weight, height):
    '''
    Calculates body surface area using the Mosteller formula, given in m2
    Parameters
    ----------
    weight : given in kg
    height : given in meters
    '''
    return math.sqrt((weight * height * 100) / 3600)

def pulse_pressure(systolic_blood_pressure, diastolic_blood_pressure):
    '''
    Calculates pulse pressure given in mmHg
    Parameters
    ----------
    systolic_blood_pressure : given in mmHg
    diastolic_blood_pressure : given in mmHg
    '''
    return systolic_blood_pressure - diastolic_blood_pressure

def mean_arterial_pressure(systolic_blood_pressure, diastolic_blood_pressure):
    '''
    Calculates mean arterial pressure given in mmHg
    Parameters
    ----------
    systolic_blood_pressure : given in mmHg
    diastolic_blood_pressure : given in mmHg
    '''
    return diastolic_blood_pressure + (pulse_pressure(systolic_blood_pressure, diastolic_blood_pressure)) * 0.35

def pressure_dependent_arterial_compliance(age, weight, height, systolic_blood_pressure, diastolic_blood_pressure):
    '''
    Calculates pressure-dependent arterial compliance C(p) given in ml/mmHg
    Parameters
    ----------
    age : given in years
    weight : given in kg
    height : given in meters
    systolic_blood_pressure : given in mmHg
    diastolic_blood_pressure : given in mmHg
    '''
    body_mass_index_ = body_mass_index(weight, height)
    intermediary = (
        (
            mean_arterial_pressure(systolic_blood_pressure, diastolic_blood_pressure)
            - (76 - 0.89 * age)
        ) / (57 - 0.44 * age)
    )
    intermediary = 5.62 / (math.pi * (57 - 0.44 * age)) / (1 + (intermediary ** 2))
    output = (height * 100 / 2) * intermediary
    output = output * (body_mass_index_ / 27.5)
    return output

def characteristic_impedance(age, weight, height, systolic_blood_pressure, diastolic_blood_pressure):
    '''
    Calculates the characteristic impedance (Zc) given in mmHg.s/cm3
    It outputs Zc by default. It outputs Zc of mayor area if prompted
    Parameters
    ----------
    age : given in years
    weight : given in kg
    height : given in meters
    systolic_blood_pressure : given in mmHg
    diastolic_blood_pressure : given in mmHg
    '''
    compliance = pressure_dependent_arterial_compliance(age, weight, height, systolic_blood_pressure, diastolic_blood_pressure)
    intermediary_1 = (
        (
            mean_arterial_pressure(systolic_blood_pressure, diastolic_blood_pressure)
            - (76 - 0.89 * age)
        ) / (57 - 0.44 * age)
    )
    intermediary_1 = math.atan(intermediary_1)# / 0.7854
    intermediary = 5.62 *(0.5 + (1 / math.pi * intermediary_1))
    output = math.sqrt(1.06 / (intermediary * compliance))
    return output

def tau_rc(systolic_blood_pressure, diastolic_blood_pressure, heart_rate):
    '''
    Calculates the RC time constant given in seconds
    Parameters
    ----------
    systolic_blood_pressure : given in mmHg
    diastolic_blood_pressure : given in mmHg
    heart_rate : given in beats per minute
    '''
    mean_arterial_pressure_ = mean_arterial_pressure(systolic_blood_pressure, diastolic_blood_pressure)
    pulse_pressure_ = pulse_pressure(systolic_blood_pressure, diastolic_blood_pressure)
    return (mean_arterial_pressure_ / pulse_pressure_) * (60 / heart_rate)

def tau_wk(systolic_blood_pressure, diastolic_blood_pressure, heart_rate):
    '''
    Calculates the RC time constant given in seconds using the winkesel model
    Parameters
    ----------
    systolic_blood_pressure : given in mmHg
    diastolic_blood_pressure : given in mmHg
    heart_rate : given in beats per minute
    '''
    mean_arterial_pressure_ = mean_arterial_pressure(systolic_blood_pressure, diastolic_blood_pressure)
    ejection_time = (((413 - 1.7 * heart_rate) / math.sqrt( mean_arterial_pressure_ / 95)) / 1000)
    return ((60 / heart_rate) - ejection_time) / math.log((mean_arterial_pressure_ / diastolic_blood_pressure))

def stroke_volume(age, weight, height, systolic_blood_pressure, diastolic_blood_pressure, heart_rate):
    '''
    Calculates the stroke volume(sv) given in milliliters (ml)
    Parameters
    ----------
    age : given in years
    weight : given in kg
    height : given in meters
    systolic_blood_pressure : given in mmHg
    diastolic_blood_pressure : given in mmHg
    heart_rate : given in beats per minute
    '''
    impedance = characteristic_impedance(age, weight, height, systolic_blood_pressure, diastolic_blood_pressure)
    mean_arterial_pressure_ = mean_arterial_pressure(systolic_blood_pressure, diastolic_blood_pressure)
    ejection_time = (((413 - 1.7 * heart_rate) / math.sqrt( mean_arterial_pressure_ / 95)) / 1000)
    end_diastolic_blood_pressure_tau = mean_arterial_pressure_ * (math.e ** ((- ejection_time / (heart_rate / 60)) - 0.25))
    end_diastolic_blood_pressure = mean_arterial_pressure_ - end_diastolic_blood_pressure_tau
    return end_diastolic_blood_pressure / impedance

def cardiac_output(age, weight, height, systolic_blood_pressure, diastolic_blood_pressure, heart_rate):
    '''
    Calculates the cardiac output (co) given in liliters per minute (L/min)
    Parameters
    ----------
    age : given in years
    weight : given in kg
    height : given in meters
    systolic_blood_pressure : given in mmHg
    diastolic_blood_pressure : given in mmHg
    heart_rate : given in beats per minute
    '''
    stroke_volume_ = stroke_volume(age, weight, height, systolic_blood_pressure, diastolic_blood_pressure, heart_rate)
    return (stroke_volume_ / 1000) * heart_rate

def cardiac_index(age, weight, height, systolic_blood_pressure, diastolic_blood_pressure, heart_rate):
    '''
    Calculates the cardiac output (co) given in liliters per minute per m2 (L/min)
    Parameters
    ----------
    age : given in years
    weight : given in kg
    height : given in meters
    systolic_blood_pressure : given in mmHg
    diastolic_blood_pressure : given in mmHg
    heart_rate : given in beats per minute
    '''
    body_surface_area_ = body_surface_area(weight, height)
    cardiac_output_ = cardiac_output(age, weight, height, systolic_blood_pressure, diastolic_blood_pressure, heart_rate)
    return cardiac_output_ / body_surface_area_

def pulse_wave_velocity(age, systolic_blood_pressure, diastolic_blood_pressure):
    '''
    Calculates pulse wave velocity
    Parameters
    ----------
    age : given in years
    systolic_blood_pressure : given in mmHg
    diastolic_blood_pressure : given in mmHg
    '''
    intermediary_1 = (
        (
            mean_arterial_pressure(systolic_blood_pressure, diastolic_blood_pressure)
            - (76 - 0.89 * age)
        ) / (57 - 0.44 * age)
    )

    intermediary_2 = (1+(intermediary_1**2))
    intermediary_3 = ((1/math.pi) * math.atan(intermediary_1))
    output = (0.357 * math.sqrt((math.pi * (57 - 0.44 * age)) * intermediary_2 * (0.5 + intermediary_3)))
    return output

def systemic_vascular_resistance(age, weight, height, systolic_blood_pressure, diastolic_blood_pressure, heart_rate):
    '''
    Calculates the systemic vasclar resistance given in dyn × s/cm-5
    Parameters
    ----------
    age : given in years
    weight : given in kg
    height : given in meters
    systolic_blood_pressure : given in mmHg
    diastolic_blood_pressure : given in mmHg
    heart_rate : given in beats per minute
    '''
    sympathetic_nervous_system_activation_ = sympathetic_nervous_system_activation(age, weight, height, systolic_blood_pressure, diastolic_blood_pressure, heart_rate)
    mean_arterial_pressure_ = mean_arterial_pressure(systolic_blood_pressure, diastolic_blood_pressure)
    cardiac_output_ = cardiac_output(age, weight, height, systolic_blood_pressure, diastolic_blood_pressure, heart_rate)
    return ((1 - (1 / sympathetic_nervous_system_activation_)) * (mean_arterial_pressure_ / cardiac_output_)) * 80

def sympathetic_activity_index(age, weight, height, systolic_blood_pressure, diastolic_blood_pressure, heart_rate):
    '''
    Calculates the sympathetic activity index given in percentage %
    Parameters
    ----------
    age : given in years
    weight : given in kg
    height : given in meters
    systolic_blood_pressure : given in mmHg
    diastolic_blood_pressure : given in mmHg
    heart_rate : given in beats per minute
    '''
    sympathetic_nervous_system_activation_ = sympathetic_nervous_system_activation(age, weight, height, systolic_blood_pressure, diastolic_blood_pressure, heart_rate)
    return (1 / sympathetic_nervous_system_activation_) * 100

def baroreflex_activity(age, weight, height, systolic_blood_pressure, diastolic_blood_pressure, heart_rate):
    '''
    Calculates the baroreflex activity given in U
    Parameters
    ----------
    age : given in years
    weight : given in kg
    height : given in meters
    systolic_blood_pressure : given in mmHg
    diastolic_blood_pressure : given in mmHg
    heart_rate : given in beats per minute
    '''
    sympathetic_nervous_system_activation_ = sympathetic_nervous_system_activation(age, weight, height, systolic_blood_pressure, diastolic_blood_pressure, heart_rate)
    baroreflex_heart_rate_ = baroreflex_heart_rate(systolic_blood_pressure, diastolic_blood_pressure)
    intermediary = (sympathetic_nervous_system_activation_ * 0.75) - (baroreflex_heart_rate_  * 0.25)
    if intermediary < 0:
        return 0
    else:
        return math.sqrt(intermediary / 7.7)

def maximum_elastance(age, weight, height, systolic_blood_pressure, diastolic_blood_pressure, heart_rate, left_ventricular_ejection_fraction=0.65):
    '''
    Calculates the maximum_elastance
    Parameters
    ----------
    age : given in years
    weight : given in kg
    height : given in meters
    systolic_blood_pressure : given in mmHg
    diastolic_blood_pressure : given in mmHg
    heart_rate : given in beats per minute
    left_ventricular_ejection_fraction : given as a the fraction of stroke volume over end diastolic volume
    '''
    stroke_volume_ = stroke_volume(age, weight, height, systolic_blood_pressure, diastolic_blood_pressure, heart_rate)
    mean_arterial_pressure_ = mean_arterial_pressure(systolic_blood_pressure, diastolic_blood_pressure)
    pep = ((131-0.4 * heart_rate) * math.sqrt( mean_arterial_pressure_ / 100)) / 1000
    ejection_time = (((413 - 1.7 * heart_rate) / math.sqrt( mean_arterial_pressure_ / 95)) / 1000)
    ratio_pep_sistolic_time = pep / ejection_time
    elastance_c = [
        0.35695,
        (-7.2266),
        74.249,
        (-307.39),
        684.54,
        (-856.92),
        571.95,
        (-159.1)
    ]
    elastance_nd_mean = 0
    for i in elastance_c:
        elastance_nd_mean += (ratio_pep_sistolic_time * i)

    elastance_nd_est = (
        (0.0275 - (0.165 * left_ventricular_ejection_fraction))
        + (0.3656 * (diastolic_blood_pressure
        / systolic_blood_pressure))
        + (0.515 * elastance_nd_mean)
    )
    elastance_es_sb = (
        (diastolic_blood_pressure - (elastance_nd_est * 0.9 * systolic_blood_pressure))
        /(stroke_volume_ * elastance_nd_est)
        )
    return 0.78 * elastance_es_sb + 0.55

def arterial_elastance(age, weight, height, systolic_blood_pressure, diastolic_blood_pressure, heart_rate):
    '''
    Calculates the arterial elastance
    Parameters
    ----------
    age : given in years
    weight : given in kg
    height : given in meters
    systolic_blood_pressure : given in mmHg
    diastolic_blood_pressure : given in mmHg
    heart_rate : given in beats per minute
    '''
    stroke_volume_ = stroke_volume(age, weight, height, systolic_blood_pressure, diastolic_blood_pressure, heart_rate)
    mean_arterial_pressure_ = mean_arterial_pressure(systolic_blood_pressure, diastolic_blood_pressure)
    return mean_arterial_pressure_ / stroke_volume_

def arterial_ventricular_elastance(
    age,
    weight,
    height,
    systolic_blood_pressure,
    diastolic_blood_pressure,
    heart_rate,
    left_ventricular_ejection_fraction=0.65
    ):
    '''
    Calculates the arterial elastance over the maximum elastance
    Parameters
    ----------
    age : given in years
    weight : given in kg
    height : given in meters
    systolic_blood_pressure : given in mmHg
    diastolic_blood_pressure : given in mmHg
    heart_rate : given in beats per minute
    left_ventricular_ejection_fraction : given as a the fraction of stroke volume over end diastolic volume
    '''
    maximum_elastance_ = maximum_elastance(
        age,
        weight,
        height,
        systolic_blood_pressure,
        diastolic_blood_pressure,
        heart_rate,
        left_ventricular_ejection_fraction=0.65
        )
    arterial_elastance_ =   arterial_elastance(
        age,
        weight,
        height,
        systolic_blood_pressure,
        diastolic_blood_pressure,
        heart_rate
        )
    return arterial_elastance_ / maximum_elastance_

def pulsatile_load(age, weight, height, systolic_blood_pressure, diastolic_blood_pressure, heart_rate):
    '''
    Calculates the pulsatile charge
    Parameters
    ----------
    age : given in years
    weight : given in kg
    height : given in meters
    systolic_blood_pressure : given in mmHg
    diastolic_blood_pressure : given in mmHg
    heart_rate : given in beats per minute
    '''
    stroke_volume_ = stroke_volume(age, weight, height, systolic_blood_pressure, diastolic_blood_pressure, heart_rate)
    pulse_pressure_ = pulse_pressure(systolic_blood_pressure, diastolic_blood_pressure)
    return pulse_pressure_ / stroke_volume_

def cardiac_potency(age, weight, height, systolic_blood_pressure, diastolic_blood_pressure, heart_rate):
    '''
    Calculates the cardiac potency
    Parameters
    ----------
    age : given in years
    weight : given in kg
    height : given in meters
    systolic_blood_pressure : given in mmHg
    diastolic_blood_pressure : given in mmHg
    heart_rate : given in beats per minute
    '''
    cardiac_output_ = cardiac_output(age, weight, height, systolic_blood_pressure, diastolic_blood_pressure, heart_rate)
    mean_arterial_pressure_ = mean_arterial_pressure(systolic_blood_pressure, diastolic_blood_pressure)
    return (cardiac_output_ * mean_arterial_pressure_)/ 450

def sympathetic_nervous_system_activation(age, weight, height, systolic_blood_pressure, diastolic_blood_pressure, heart_rate):
    '''
    Calculates the sympathetic nervous system activation
    Parameters
    ----------
    age : given in years
    weight : given in kg
    height : given in meters
    systolic_blood_pressure : given in mmHg
    diastolic_blood_pressure : given in mmHg
    heart_rate : given in beats per minute
    '''
    impedance = characteristic_impedance(age, weight, height, systolic_blood_pressure, diastolic_blood_pressure)
    return math.e**(((60 / heart_rate) + 0.12) / impedance)

def baroreflex_heart_rate(systolic_blood_pressure, diastolic_blood_pressure):
    '''
    baroreflex_heart_rate
    Parameters
    ----------
    systolic_blood_pressure : given in mmHg
    diastolic_blood_pressure : given in mmHg
    '''
    mean_arterial_pressure_ = mean_arterial_pressure(systolic_blood_pressure, diastolic_blood_pressure)
    return 0.66 + ((0.66 - 1.2) / (1 + 67000000000000 * math.e ** (-31 * mean_arterial_pressure_ / 89)))
