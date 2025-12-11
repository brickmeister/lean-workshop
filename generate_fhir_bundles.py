#!/usr/bin/env python3
"""
Generate 100 FHIR R4 patient bundles with comprehensive clinical data.
Each bundle contains all resources for a single patient.
"""

import json
import random
import os
from datetime import datetime, timedelta
from pathlib import Path

# Sample data pools
FIRST_NAMES_FEMALE = [
    "Emma", "Olivia", "Ava", "Isabella", "Sophia", "Mia", "Charlotte", "Amelia", "Harper", "Evelyn",
    "Abigail", "Emily", "Elizabeth", "Sofia", "Avery", "Ella", "Scarlett", "Grace", "Chloe", "Victoria",
    "Riley", "Aria", "Lily", "Aurora", "Zoey", "Nora", "Camila", "Hannah", "Lillian", "Addison",
    "Eleanor", "Natalie", "Luna", "Savannah", "Brooklyn", "Leah", "Zoe", "Stella", "Hazel", "Ellie",
    "Paisley", "Audrey", "Skylar", "Violet", "Claire", "Bella", "Lucy", "Anna", "Caroline", "Genesis"
]

FIRST_NAMES_MALE = [
    "Liam", "Noah", "Oliver", "James", "Elijah", "William", "Henry", "Lucas", "Benjamin", "Theodore",
    "Jack", "Levi", "Alexander", "Mason", "Ethan", "Daniel", "Jacob", "Michael", "Logan", "Jackson",
    "Sebastian", "Aiden", "Owen", "Samuel", "Ryan", "Nathan", "Carter", "Luke", "Jayden", "John",
    "David", "Leo", "Isaac", "Dylan", "Jaxon", "Gabriel", "Anthony", "Julian", "Mateo", "Wyatt",
    "Joshua", "Asher", "Christopher", "Andrew", "Lincoln", "Thomas", "Maverick", "Josiah", "Charles", "Caleb"
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
    "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
    "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson",
    "Walker", "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
    "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell", "Carter", "Roberts",
    "Gomez", "Phillips", "Evans", "Turner", "Diaz", "Parker", "Cruz", "Edwards", "Collins", "Reyes",
    "Stewart", "Morris", "Morales", "Murphy", "Cook", "Rogers", "Gutierrez", "Ortiz", "Morgan", "Cooper",
    "Peterson", "Bailey", "Reed", "Kelly", "Howard", "Ramos", "Kim", "Cox", "Ward", "Richardson",
    "Watson", "Brooks", "Chavez", "Wood", "James", "Bennett", "Gray", "Mendoza", "Ruiz", "Hughes",
    "Price", "Alvarez", "Castillo", "Sanders", "Patel", "Myers", "Long", "Ross", "Foster", "Jimenez"
]

CITIES = [
    ("Austin", "TX", "78701"), ("Seattle", "WA", "98101"), ("Denver", "CO", "80202"),
    ("San Francisco", "CA", "94102"), ("Chicago", "IL", "60601"), ("Boston", "MA", "02101"),
    ("New York", "NY", "10001"), ("Los Angeles", "CA", "90001"), ("Phoenix", "AZ", "85001"),
    ("Philadelphia", "PA", "19101"), ("San Antonio", "TX", "78201"), ("San Diego", "CA", "92101"),
    ("Dallas", "TX", "75201"), ("Portland", "OR", "97201"), ("Miami", "FL", "33101"),
    ("Atlanta", "GA", "30301"), ("Nashville", "TN", "37201"), ("Detroit", "MI", "48201"),
    ("Minneapolis", "MN", "55401"), ("Cleveland", "OH", "44101")
]

STREET_NAMES = [
    "Oak", "Maple", "Cedar", "Pine", "Elm", "Walnut", "Cherry", "Birch", "Willow", "Magnolia",
    "Main", "First", "Second", "Third", "Park", "Washington", "Lake", "Hill", "Forest", "River"
]

STREET_TYPES = ["Street", "Avenue", "Drive", "Lane", "Road", "Boulevard", "Court", "Way", "Place", "Circle"]

CONDITIONS = [
    ("38341003", "I10", "Essential hypertension", "Hypertensive disorder"),
    ("44054006", "E11.9", "Type 2 diabetes mellitus", "Type 2 diabetes mellitus"),
    ("195967001", "J45.909", "Asthma", "Asthma, unspecified"),
    ("13645005", "J06.9", "Upper respiratory infection", "Acute upper respiratory infection"),
    ("40055000", "K21.0", "GERD", "Gastro-esophageal reflux disease with esophagitis"),
    ("386661006", "R50.9", "Fever", "Fever, unspecified"),
    ("25064002", "G43.909", "Migraine", "Migraine, unspecified"),
    ("73211009", "E78.5", "Hyperlipidemia", "Hyperlipidemia, unspecified"),
    ("35489007", "F32.9", "Depression", "Major depressive disorder, single episode"),
    ("197480006", "M54.5", "Low back pain", "Low back pain"),
    ("233604007", "I25.10", "Coronary artery disease", "Atherosclerotic heart disease"),
    ("84114007", "N39.0", "Urinary tract infection", "Urinary tract infection"),
    ("267036007", "R05.9", "Chronic cough", "Cough, unspecified"),
    ("49436004", "J00", "Common cold", "Acute nasopharyngitis"),
    ("271737000", "D64.9", "Anemia", "Anemia, unspecified"),
    ("56265001", "I48.91", "Atrial fibrillation", "Unspecified atrial fibrillation"),
    ("64859006", "M17.9", "Osteoarthritis", "Osteoarthritis of knee"),
    ("230690007", "G47.00", "Insomnia", "Insomnia, unspecified"),
    ("59621000", "I10", "Stage 1 hypertension", "Essential hypertension"),
    ("73430006", "R73.03", "Prediabetes", "Prediabetes")
]

MEDICATIONS = [
    ("197361", "Amlodipine 5 MG Oral Tablet", "5", "mg", "once daily"),
    ("860975", "Metformin 500 MG Oral Tablet", "500", "mg", "twice daily"),
    ("311995", "Lisinopril 10 MG Oral Tablet", "10", "mg", "once daily"),
    ("198211", "Aspirin 81 MG Oral Tablet", "81", "mg", "once daily"),
    ("861007", "Metoprolol Succinate 25 MG Extended Release Oral Tablet", "25", "mg", "once daily"),
    ("310798", "Atorvastatin 20 MG Oral Tablet", "20", "mg", "once daily at bedtime"),
    ("197517", "Omeprazole 20 MG Delayed Release Oral Capsule", "20", "mg", "once daily before breakfast"),
    ("312961", "Levothyroxine 50 MCG Oral Tablet", "50", "mcg", "once daily in the morning"),
    ("855332", "Albuterol 90 MCG/ACTUAT Metered Dose Inhaler", "90", "mcg", "as needed for shortness of breath"),
    ("904420", "Sertraline 50 MG Oral Tablet", "50", "mg", "once daily"),
    ("197591", "Hydrochlorothiazide 25 MG Oral Tablet", "25", "mg", "once daily"),
    ("310429", "Gabapentin 300 MG Oral Capsule", "300", "mg", "three times daily"),
    ("198240", "Acetaminophen 500 MG Oral Tablet", "500", "mg", "every 6 hours as needed"),
    ("197381", "Amlodipine 10 MG Oral Tablet", "10", "mg", "once daily"),
    ("312617", "Losartan 50 MG Oral Tablet", "50", "mg", "once daily")
]

LAB_TESTS = [
    ("2093-3", "Total Cholesterol", "mg/dL", 150, 240, 200),
    ("2571-8", "Triglycerides", "mg/dL", 80, 200, 150),
    ("2085-9", "HDL Cholesterol", "mg/dL", 35, 80, 40),
    ("13457-7", "LDL Cholesterol", "mg/dL", 70, 180, 100),
    ("4548-4", "HbA1c", "%", 4.5, 9.0, 5.7),
    ("2339-0", "Glucose", "mg/dL", 70, 150, 100),
    ("2160-0", "Creatinine", "mg/dL", 0.6, 1.5, 1.2),
    ("3094-0", "BUN", "mg/dL", 7, 25, 20),
    ("2951-2", "Sodium", "mmol/L", 136, 148, 145),
    ("2823-3", "Potassium", "mmol/L", 3.5, 5.5, 5.0),
    ("718-7", "Hemoglobin", "g/dL", 11, 17, 12),
    ("787-2", "MCV", "fL", 75, 100, 80),
    ("6690-2", "WBC", "10*3/uL", 4, 12, 10),
    ("777-3", "Platelets", "10*3/uL", 150, 400, 150)
]

VITAL_SIGNS = [
    ("8480-6", "Systolic BP", "mm[Hg]", "mmHg", 100, 160),
    ("8462-4", "Diastolic BP", "mm[Hg]", "mmHg", 60, 100),
    ("8867-4", "Heart Rate", "/min", "beats/min", 55, 100),
    ("8310-5", "Body Temperature", "[degF]", "°F", 97.0, 99.5),
    ("9279-1", "Respiratory Rate", "/min", "breaths/min", 12, 20),
    ("2708-6", "Oxygen Saturation", "%", "%", 94, 100),
    ("29463-7", "Body Weight", "kg", "kg", 50, 120),
    ("8302-2", "Body Height", "cm", "cm", 150, 195)
]

ENCOUNTER_TYPES = [
    ("185349003", "Encounter for check up", "Annual Physical"),
    ("390906007", "Follow-up encounter", "Follow-up Visit"),
    ("185345009", "Encounter for symptom", "Sick Visit"),
    ("50849002", "Emergency room admission", "Emergency Visit"),
    ("439740005", "Postoperative follow-up visit", "Post-op Follow-up"),
    ("270427003", "Patient-initiated encounter", "Walk-in Visit"),
    ("308335008", "Patient encounter procedure", "Procedure Visit"),
    ("371883000", "Outpatient procedure", "Outpatient Procedure")
]

PRACTITIONERS = [
    ("practitioner-001", "Dr. Sarah Williams", "MD"),
    ("practitioner-002", "Dr. Michael Chen", "MD"),
    ("practitioner-003", "Dr. Emily Rodriguez", "MD"),
    ("practitioner-004", "Dr. James Thompson", "MD"),
    ("practitioner-005", "Dr. Lisa Patel", "MD"),
    ("practitioner-006", "Dr. Robert Kim", "MD"),
    ("practitioner-007", "Dr. Jennifer Martinez", "MD"),
    ("practitioner-008", "Dr. David Wilson", "MD"),
    ("practitioner-009", "Dr. Amanda Foster", "MD"),
    ("practitioner-010", "Dr. Christopher Lee", "MD")
]

ORGANIZATIONS = [
    ("org-001", "Austin Primary Care Clinic", "Austin", "TX"),
    ("org-002", "Seattle General Hospital", "Seattle", "WA"),
    ("org-003", "Denver Medical Center", "Denver", "CO"),
    ("org-004", "Bay Area Health Partners", "San Francisco", "CA"),
    ("org-005", "Chicago Family Medicine", "Chicago", "IL"),
    ("org-006", "Boston Healthcare Associates", "Boston", "MA"),
    ("org-007", "NYC Medical Group", "New York", "NY"),
    ("org-008", "LA Community Health", "Los Angeles", "CA"),
    ("org-009", "Phoenix Care Network", "Phoenix", "AZ"),
    ("org-010", "Miami Health Center", "Miami", "FL")
]


def random_date(start_year=1940, end_year=2005):
    """Generate a random birth date."""
    year = random.randint(start_year, end_year)
    month = random.randint(1, 12)
    day = random.randint(1, 28)
    return f"{year}-{month:02d}-{day:02d}"


def random_phone():
    """Generate a random phone number."""
    return f"({random.randint(200, 999)}) {random.randint(200, 999)}-{random.randint(1000, 9999)}"


def random_datetime(days_back=365):
    """Generate a random datetime within the past N days."""
    base = datetime.now() - timedelta(days=random.randint(0, days_back))
    return base.strftime("%Y-%m-%dT%H:%M:%SZ")


def sanitize_filename(name):
    """Create a safe filename from a patient name."""
    return name.lower().replace(" ", "_").replace(".", "").replace(",", "")


def generate_patient(patient_id, first_name, last_name, gender, birth_date, city_info):
    """Generate a FHIR Patient resource."""
    city, state, postal = city_info
    street_num = random.randint(100, 9999)
    street = f"{street_num} {random.choice(STREET_NAMES)} {random.choice(STREET_TYPES)}"
    
    return {
        "resourceType": "Patient",
        "id": patient_id,
        "meta": {"lastUpdated": random_datetime(30)},
        "identifier": [
            {"system": "http://hospital.example.org/patient-id", "value": f"PT-{patient_id}"},
            {"system": "http://hl7.org/fhir/sid/us-ssn", "value": f"{random.randint(100, 999)}-{random.randint(10, 99)}-{random.randint(1000, 9999)}"}
        ],
        "active": True,
        "name": [{"use": "official", "family": last_name, "given": [first_name]}],
        "telecom": [
            {"system": "phone", "value": random_phone(), "use": "home"},
            {"system": "email", "value": f"{first_name.lower()}.{last_name.lower()}@email.com", "use": "home"}
        ],
        "gender": gender,
        "birthDate": birth_date,
        "address": [{
            "use": "home",
            "line": [street],
            "city": city,
            "state": state,
            "postalCode": postal,
            "country": "USA"
        }],
        "maritalStatus": {
            "coding": [{
                "system": "http://terminology.hl7.org/CodeSystem/v3-MaritalStatus",
                "code": random.choice(["S", "M", "D", "W"]),
                "display": random.choice(["Never Married", "Married", "Divorced", "Widowed"])
            }]
        }
    }


def generate_encounter(encounter_id, patient_id, patient_name, encounter_type, practitioner, organization, encounter_date):
    """Generate a FHIR Encounter resource."""
    snomed, display, text = encounter_type
    prac_id, prac_name, _ = practitioner
    org_id, org_name, _, _ = organization
    
    end_date = (datetime.fromisoformat(encounter_date.replace("Z", "")) + timedelta(minutes=random.randint(15, 120))).strftime("%Y-%m-%dT%H:%M:%SZ")
    
    return {
        "resourceType": "Encounter",
        "id": encounter_id,
        "meta": {"lastUpdated": encounter_date},
        "identifier": [{"system": "http://hospital.example.org/encounter-id", "value": f"ENC-{encounter_id}"}],
        "status": "finished",
        "class": {
            "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
            "code": "AMB",
            "display": "ambulatory"
        },
        "type": [{
            "coding": [{"system": "http://snomed.info/sct", "code": snomed, "display": display}],
            "text": text
        }],
        "subject": {"reference": f"Patient/{patient_id}", "display": patient_name},
        "participant": [{
            "type": [{"coding": [{"system": "http://terminology.hl7.org/CodeSystem/v3-ParticipationType", "code": "PPRF", "display": "primary performer"}]}],
            "individual": {"reference": f"Practitioner/{prac_id}", "display": prac_name}
        }],
        "period": {"start": encounter_date, "end": end_date},
        "serviceProvider": {"reference": f"Organization/{org_id}", "display": org_name}
    }


def generate_condition(condition_id, patient_id, patient_name, encounter_id, condition_info, onset_date):
    """Generate a FHIR Condition resource."""
    snomed, icd10, display, text = condition_info
    
    return {
        "resourceType": "Condition",
        "id": condition_id,
        "meta": {"lastUpdated": random_datetime(30)},
        "clinicalStatus": {
            "coding": [{"system": "http://terminology.hl7.org/CodeSystem/condition-clinical", "code": "active", "display": "Active"}]
        },
        "verificationStatus": {
            "coding": [{"system": "http://terminology.hl7.org/CodeSystem/condition-ver-status", "code": "confirmed", "display": "Confirmed"}]
        },
        "category": [{
            "coding": [{"system": "http://terminology.hl7.org/CodeSystem/condition-category", "code": "problem-list-item", "display": "Problem List Item"}]
        }],
        "code": {
            "coding": [
                {"system": "http://snomed.info/sct", "code": snomed, "display": display},
                {"system": "http://hl7.org/fhir/sid/icd-10-cm", "code": icd10, "display": text}
            ],
            "text": text
        },
        "subject": {"reference": f"Patient/{patient_id}", "display": patient_name},
        "encounter": {"reference": f"Encounter/{encounter_id}"},
        "onsetDateTime": onset_date,
        "recordedDate": onset_date
    }


def generate_medication_request(med_id, patient_id, patient_name, encounter_id, condition_id, medication_info, authored_date):
    """Generate a FHIR MedicationRequest resource."""
    rxnorm, display, dose_val, dose_unit, instructions = medication_info
    
    return {
        "resourceType": "MedicationRequest",
        "id": med_id,
        "meta": {"lastUpdated": random_datetime(30)},
        "identifier": [{"system": "http://hospital.example.org/rx-id", "value": f"RX-{med_id}"}],
        "status": "active",
        "intent": "order",
        "category": [{
            "coding": [{"system": "http://terminology.hl7.org/CodeSystem/medicationrequest-category", "code": "outpatient", "display": "Outpatient"}]
        }],
        "medicationCodeableConcept": {
            "coding": [{"system": "http://www.nlm.nih.gov/research/umls/rxnorm", "code": rxnorm, "display": display}],
            "text": display
        },
        "subject": {"reference": f"Patient/{patient_id}", "display": patient_name},
        "encounter": {"reference": f"Encounter/{encounter_id}"},
        "authoredOn": authored_date,
        "reasonReference": [{"reference": f"Condition/{condition_id}"}],
        "dosageInstruction": [{
            "text": f"Take {instructions}",
            "route": {"coding": [{"system": "http://snomed.info/sct", "code": "26643006", "display": "Oral route"}]},
            "doseAndRate": [{"doseQuantity": {"value": float(dose_val), "unit": dose_unit, "system": "http://unitsofmeasure.org"}}]
        }],
        "dispenseRequest": {
            "numberOfRepeatsAllowed": random.randint(0, 11),
            "quantity": {"value": 30, "unit": "tablets"},
            "expectedSupplyDuration": {"value": 30, "unit": "days", "system": "http://unitsofmeasure.org", "code": "d"}
        },
        "substitution": {"allowedBoolean": True}
    }


def generate_observation_lab(obs_id, patient_id, patient_name, encounter_id, lab_info, effective_date):
    """Generate a FHIR Observation resource for lab results."""
    loinc, display, unit, low, high, ref_high = lab_info
    value = round(random.uniform(low, high), 1)
    
    interpretation = "N"
    if value > ref_high:
        interpretation = "H"
    elif value < low + (ref_high - low) * 0.1:
        interpretation = "L"
    
    return {
        "resourceType": "Observation",
        "id": obs_id,
        "meta": {"lastUpdated": random_datetime(30)},
        "status": "final",
        "category": [{
            "coding": [{"system": "http://terminology.hl7.org/CodeSystem/observation-category", "code": "laboratory", "display": "Laboratory"}]
        }],
        "code": {
            "coding": [{"system": "http://loinc.org", "code": loinc, "display": display}],
            "text": display
        },
        "subject": {"reference": f"Patient/{patient_id}", "display": patient_name},
        "encounter": {"reference": f"Encounter/{encounter_id}"},
        "effectiveDateTime": effective_date,
        "valueQuantity": {"value": value, "unit": unit, "system": "http://unitsofmeasure.org"},
        "interpretation": [{
            "coding": [{"system": "http://terminology.hl7.org/CodeSystem/v3-ObservationInterpretation", "code": interpretation}]
        }],
        "referenceRange": [{"low": {"value": low, "unit": unit}, "high": {"value": ref_high, "unit": unit}}]
    }


def generate_observation_vital(obs_id, patient_id, patient_name, encounter_id, vital_info, effective_date):
    """Generate a FHIR Observation resource for vital signs."""
    loinc, display, code, unit, low, high = vital_info
    value = round(random.uniform(low, high), 1)
    
    return {
        "resourceType": "Observation",
        "id": obs_id,
        "meta": {"lastUpdated": random_datetime(30)},
        "status": "final",
        "category": [{
            "coding": [{"system": "http://terminology.hl7.org/CodeSystem/observation-category", "code": "vital-signs", "display": "Vital Signs"}]
        }],
        "code": {
            "coding": [{"system": "http://loinc.org", "code": loinc, "display": display}],
            "text": display
        },
        "subject": {"reference": f"Patient/{patient_id}", "display": patient_name},
        "encounter": {"reference": f"Encounter/{encounter_id}"},
        "effectiveDateTime": effective_date,
        "valueQuantity": {"value": value, "unit": unit, "system": "http://unitsofmeasure.org", "code": code}
    }


def generate_patient_bundle(patient_num):
    """Generate a complete FHIR bundle for a single patient."""
    # Determine patient demographics
    gender = random.choice(["male", "female"])
    if gender == "female":
        first_name = random.choice(FIRST_NAMES_FEMALE)
    else:
        first_name = random.choice(FIRST_NAMES_MALE)
    
    last_name = random.choice(LAST_NAMES)
    birth_date = random_date()
    city_info = random.choice(CITIES)
    
    patient_id = f"{patient_num:04d}"
    patient_name = f"{first_name} {last_name}"
    
    entries = []
    
    # Add Patient resource
    patient = generate_patient(patient_id, first_name, last_name, gender, birth_date, city_info)
    entries.append({
        "fullUrl": f"urn:uuid:patient-{patient_id}",
        "resource": patient,
        "request": {"method": "PUT", "url": f"Patient/{patient_id}"}
    })
    
    # Generate 2-5 encounters
    num_encounters = random.randint(2, 5)
    encounter_dates = sorted([random_datetime(730) for _ in range(num_encounters)])
    
    for i, enc_date in enumerate(encounter_dates):
        encounter_id = f"{patient_id}-enc-{i+1:02d}"
        encounter_type = random.choice(ENCOUNTER_TYPES)
        practitioner = random.choice(PRACTITIONERS)
        organization = random.choice(ORGANIZATIONS)
        
        encounter = generate_encounter(encounter_id, patient_id, patient_name, encounter_type, practitioner, organization, enc_date)
        entries.append({
            "fullUrl": f"urn:uuid:encounter-{encounter_id}",
            "resource": encounter,
            "request": {"method": "PUT", "url": f"Encounter/{encounter_id}"}
        })
        
        # Generate vitals for each encounter
        vitals_sample = random.sample(VITAL_SIGNS, random.randint(3, 6))
        for j, vital in enumerate(vitals_sample):
            obs_id = f"{patient_id}-vital-{i+1:02d}-{j+1:02d}"
            observation = generate_observation_vital(obs_id, patient_id, patient_name, encounter_id, vital, enc_date)
            entries.append({
                "fullUrl": f"urn:uuid:observation-{obs_id}",
                "resource": observation,
                "request": {"method": "PUT", "url": f"Observation/{obs_id}"}
            })
    
    # Generate 1-4 conditions
    num_conditions = random.randint(1, 4)
    patient_conditions = random.sample(CONDITIONS, num_conditions)
    condition_ids = []
    
    for i, cond in enumerate(patient_conditions):
        condition_id = f"{patient_id}-cond-{i+1:02d}"
        condition_ids.append((condition_id, cond))
        onset = random_datetime(1095)  # Up to 3 years back
        encounter_id = f"{patient_id}-enc-01"
        
        condition = generate_condition(condition_id, patient_id, patient_name, encounter_id, cond, onset)
        entries.append({
            "fullUrl": f"urn:uuid:condition-{condition_id}",
            "resource": condition,
            "request": {"method": "PUT", "url": f"Condition/{condition_id}"}
        })
    
    # Generate medications (1 per condition usually)
    for i, (cond_id, cond_info) in enumerate(condition_ids):
        if random.random() < 0.8:  # 80% chance of medication for each condition
            med_id = f"{patient_id}-med-{i+1:02d}"
            medication = random.choice(MEDICATIONS)
            encounter_id = f"{patient_id}-enc-01"
            authored = random_datetime(365)
            
            med_request = generate_medication_request(med_id, patient_id, patient_name, encounter_id, cond_id, medication, authored)
            entries.append({
                "fullUrl": f"urn:uuid:medicationrequest-{med_id}",
                "resource": med_request,
                "request": {"method": "PUT", "url": f"MedicationRequest/{med_id}"}
            })
    
    # Generate lab results (3-8 per patient)
    num_labs = random.randint(3, 8)
    lab_tests_sample = random.sample(LAB_TESTS, num_labs)
    
    for i, lab in enumerate(lab_tests_sample):
        obs_id = f"{patient_id}-lab-{i+1:02d}"
        encounter_id = f"{patient_id}-enc-{random.randint(1, num_encounters):02d}"
        lab_date = random_datetime(365)
        
        observation = generate_observation_lab(obs_id, patient_id, patient_name, encounter_id, lab, lab_date)
        entries.append({
            "fullUrl": f"urn:uuid:observation-{obs_id}",
            "resource": observation,
            "request": {"method": "PUT", "url": f"Observation/{obs_id}"}
        })
    
    # Create the bundle
    bundle = {
        "resourceType": "Bundle",
        "id": f"patient-bundle-{patient_id}",
        "type": "batch",
        "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "entry": entries
    }
    
    return bundle, patient_name


def main():
    """Generate 100 patient bundles."""
    output_dir = Path(__file__).parent / "data"
    output_dir.mkdir(exist_ok=True)
    
    # Remove old bundle files
    for f in output_dir.glob("*.json"):
        f.unlink()
    
    print("Generating 100 FHIR patient bundles...")
    
    generated_names = set()
    
    for i in range(1, 101):
        bundle, patient_name = generate_patient_bundle(i)
        
        # Ensure unique filenames
        base_filename = sanitize_filename(patient_name)
        filename = base_filename
        counter = 1
        while filename in generated_names:
            filename = f"{base_filename}_{counter}"
            counter += 1
        generated_names.add(filename)
        
        filepath = output_dir / f"{filename}.json"
        
        with open(filepath, "w") as f:
            json.dump(bundle, f, indent=2)
        
        resource_count = len(bundle["entry"])
        print(f"  [{i:3d}/100] {filepath.name} - {resource_count} resources")
    
    print(f"\nDone! Generated 100 bundles in {output_dir}")


if __name__ == "__main__":
    main()

