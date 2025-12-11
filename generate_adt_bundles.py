#!/usr/bin/env python3
"""
Generate 100 FHIR R4 ADT (Admit, Discharge, Transfer) message bundles.
Each bundle contains multiple ADT events for a single patient.
"""

import json
import random
import uuid
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

# ADT Event Types (HL7v2 mapped to FHIR)
ADT_EVENTS = [
    ("A01", "ADT/ACK - Admit/Visit Notification", "admit"),
    ("A02", "ADT/ACK - Transfer a Patient", "transfer"),
    ("A03", "ADT/ACK - Discharge/End Visit", "discharge"),
    ("A04", "ADT/ACK - Register a Patient", "register"),
    ("A08", "ADT/ACK - Update Patient Information", "update"),
]

# Admission reasons / diagnoses
ADMISSION_REASONS = [
    ("22298006", "Myocardial infarction", "Acute MI"),
    ("195967001", "Asthma exacerbation", "Asthma attack"),
    ("233604007", "Pneumonia", "Community-acquired pneumonia"),
    ("128053003", "Deep vein thrombosis", "DVT"),
    ("44054006", "Diabetic ketoacidosis", "DKA"),
    ("230690007", "Stroke", "Cerebrovascular accident"),
    ("73211009", "Congestive heart failure", "CHF exacerbation"),
    ("40055000", "GI bleeding", "Upper GI hemorrhage"),
    ("84114007", "Sepsis", "Septicemia"),
    ("386661006", "Fever of unknown origin", "FUO workup"),
    ("35489007", "Chest pain", "Rule out ACS"),
    ("197480006", "Acute appendicitis", "Appendicitis"),
    ("25064002", "Acute pancreatitis", "Pancreatitis"),
    ("49436004", "Hip fracture", "Femoral neck fracture"),
    ("56265001", "Atrial fibrillation", "New onset AFib"),
]

# Hospital units/wards
HOSPITAL_UNITS = [
    ("ED", "Emergency Department", "emergency"),
    ("ICU", "Intensive Care Unit", "icu"),
    ("CCU", "Cardiac Care Unit", "icu"),
    ("MICU", "Medical ICU", "icu"),
    ("SICU", "Surgical ICU", "icu"),
    ("MED", "Medical Floor", "ward"),
    ("SURG", "Surgical Floor", "ward"),
    ("TELE", "Telemetry Unit", "ward"),
    ("ORTHO", "Orthopedic Unit", "ward"),
    ("NEURO", "Neurology Unit", "ward"),
    ("PEDS", "Pediatric Unit", "ward"),
    ("OBS", "Observation Unit", "ward"),
    ("PACU", "Post-Anesthesia Care Unit", "bu"),
    ("OR", "Operating Room", "or"),
    ("CATH", "Cardiac Catheterization Lab", "bu"),
]

# Bed types
BED_TYPES = ["A", "B", "C", "D"]

# Discharge dispositions
DISCHARGE_DISPOSITIONS = [
    ("home", "Home", "Patient discharged to home"),
    ("snf", "Skilled Nursing Facility", "Discharged to SNF"),
    ("rehab", "Rehabilitation Facility", "Discharged to rehab"),
    ("hosp-trans", "Transferred to Another Hospital", "Inter-facility transfer"),
    ("asc", "Ambulatory Surgery Center", "Discharged to ASC"),
    ("long", "Long Term Care", "Discharged to LTAC"),
    ("exp", "Expired", "Patient expired"),
    ("ama", "Left Against Medical Advice", "AMA discharge"),
    ("home-health", "Home with Home Health", "Home with services"),
]

HOSPITALS = [
    ("hosp-001", "Austin General Hospital", "Austin", "TX", "1000 Hospital Drive"),
    ("hosp-002", "Seattle Medical Center", "Seattle", "WA", "2500 Healthcare Way"),
    ("hosp-003", "Denver Regional Hospital", "Denver", "CO", "500 Medical Plaza"),
    ("hosp-004", "UCSF Medical Center", "San Francisco", "CA", "505 Parnassus Ave"),
    ("hosp-005", "Northwestern Memorial", "Chicago", "IL", "251 E Huron St"),
    ("hosp-006", "Mass General Hospital", "Boston", "MA", "55 Fruit Street"),
    ("hosp-007", "NYU Langone Health", "New York", "NY", "550 First Avenue"),
    ("hosp-008", "Cedars-Sinai Medical Center", "Los Angeles", "CA", "8700 Beverly Blvd"),
    ("hosp-009", "Mayo Clinic Hospital", "Phoenix", "AZ", "5777 E Mayo Blvd"),
    ("hosp-010", "Penn Medicine", "Philadelphia", "PA", "3400 Spruce Street"),
]

ATTENDING_PHYSICIANS = [
    ("dr-001", "Dr. Sarah Williams", "Internal Medicine"),
    ("dr-002", "Dr. Michael Chen", "Emergency Medicine"),
    ("dr-003", "Dr. Emily Rodriguez", "Cardiology"),
    ("dr-004", "Dr. James Thompson", "Pulmonology"),
    ("dr-005", "Dr. Lisa Patel", "Hospitalist"),
    ("dr-006", "Dr. Robert Kim", "Surgery"),
    ("dr-007", "Dr. Jennifer Martinez", "Neurology"),
    ("dr-008", "Dr. David Wilson", "Orthopedics"),
    ("dr-009", "Dr. Amanda Foster", "Gastroenterology"),
    ("dr-010", "Dr. Christopher Lee", "Critical Care"),
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


def generate_uuid():
    """Generate a UUID."""
    return str(uuid.uuid4())


def sanitize_filename(name):
    """Create a safe filename from a patient name."""
    return name.lower().replace(" ", "_").replace(".", "").replace(",", "")


def generate_message_header(message_id, event_code, event_display, event_type, timestamp, patient_ref, encounter_ref, source_org):
    """Generate a FHIR MessageHeader for ADT event."""
    org_id, org_name, _, _, _ = source_org
    
    return {
        "resourceType": "MessageHeader",
        "id": message_id,
        "meta": {
            "lastUpdated": timestamp
        },
        "eventCoding": {
            "system": "http://terminology.hl7.org/CodeSystem/v2-0003",
            "code": event_code,
            "display": event_display
        },
        "destination": [
            {
                "name": "Hospital ADT System",
                "endpoint": f"http://{org_name.lower().replace(' ', '-')}.example.org/fhir/adt"
            }
        ],
        "source": {
            "name": org_name,
            "software": "Hospital Information System",
            "version": "5.2.1",
            "endpoint": f"http://{org_name.lower().replace(' ', '-')}.example.org/fhir"
        },
        "reason": {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/message-reasons-encounter",
                    "code": event_type,
                    "display": event_type.capitalize()
                }
            ]
        },
        "focus": [
            {"reference": f"Patient/{patient_ref}"},
            {"reference": f"Encounter/{encounter_ref}"}
        ]
    }


def generate_patient(patient_id, first_name, last_name, gender, birth_date, city_info):
    """Generate a FHIR Patient resource."""
    city, state, postal = city_info
    street_num = random.randint(100, 9999)
    street = f"{street_num} {random.choice(STREET_NAMES)} {random.choice(STREET_TYPES)}"
    
    return {
        "resourceType": "Patient",
        "id": patient_id,
        "meta": {
            "lastUpdated": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        },
        "identifier": [
            {
                "use": "usual",
                "type": {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/v2-0203",
                            "code": "MR",
                            "display": "Medical Record Number"
                        }
                    ]
                },
                "system": "http://hospital.example.org/mrn",
                "value": f"MRN-{patient_id}"
            },
            {
                "use": "official",
                "type": {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/v2-0203",
                            "code": "SS",
                            "display": "Social Security Number"
                        }
                    ]
                },
                "system": "http://hl7.org/fhir/sid/us-ssn",
                "value": f"{random.randint(100, 999)}-{random.randint(10, 99)}-{random.randint(1000, 9999)}"
            }
        ],
        "active": True,
        "name": [
            {
                "use": "official",
                "family": last_name,
                "given": [first_name]
            }
        ],
        "telecom": [
            {"system": "phone", "value": random_phone(), "use": "home"},
            {"system": "phone", "value": random_phone(), "use": "mobile"},
            {"system": "email", "value": f"{first_name.lower()}.{last_name.lower()}@email.com"}
        ],
        "gender": gender,
        "birthDate": birth_date,
        "address": [
            {
                "use": "home",
                "type": "physical",
                "line": [street],
                "city": city,
                "state": state,
                "postalCode": postal,
                "country": "USA"
            }
        ],
        "maritalStatus": {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/v3-MaritalStatus",
                    "code": random.choice(["S", "M", "D", "W"])
                }
            ]
        },
        "contact": [
            {
                "relationship": [
                    {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/v2-0131",
                                "code": "N",
                                "display": "Next-of-Kin"
                            }
                        ]
                    }
                ],
                "name": {
                    "family": last_name,
                    "given": [random.choice(FIRST_NAMES_MALE + FIRST_NAMES_FEMALE)]
                },
                "telecom": [
                    {"system": "phone", "value": random_phone()}
                ]
            }
        ]
    }


def generate_encounter(encounter_id, patient_id, patient_name, encounter_class, status, 
                       admission_reason, location, period_start, period_end, 
                       attending, hospital, discharge_disposition=None):
    """Generate a FHIR Encounter resource for ADT."""
    snomed, display, text = admission_reason
    unit_code, unit_name, unit_type = location["unit"]
    dr_id, dr_name, dr_specialty = attending
    hosp_id, hosp_name, _, _, _ = hospital
    
    encounter = {
        "resourceType": "Encounter",
        "id": encounter_id,
        "meta": {
            "lastUpdated": period_start
        },
        "identifier": [
            {
                "use": "official",
                "system": "http://hospital.example.org/encounter-id",
                "value": f"ENC-{encounter_id}"
            },
            {
                "use": "usual",
                "type": {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/v2-0203",
                            "code": "VN",
                            "display": "Visit Number"
                        }
                    ]
                },
                "system": "http://hospital.example.org/visit-number",
                "value": f"V{random.randint(100000, 999999)}"
            }
        ],
        "status": status,
        "class": {
            "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
            "code": encounter_class,
            "display": {
                "IMP": "inpatient encounter",
                "EMER": "emergency",
                "AMB": "ambulatory",
                "OBSENC": "observation encounter"
            }.get(encounter_class, "inpatient encounter")
        },
        "type": [
            {
                "coding": [
                    {
                        "system": "http://snomed.info/sct",
                        "code": "32485007",
                        "display": "Hospital admission"
                    }
                ]
            }
        ],
        "priority": {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/v3-ActPriority",
                    "code": random.choice(["EM", "UR", "R"]),
                    "display": random.choice(["emergency", "urgent", "routine"])
                }
            ]
        },
        "subject": {
            "reference": f"Patient/{patient_id}",
            "display": patient_name
        },
        "participant": [
            {
                "type": [
                    {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/v3-ParticipationType",
                                "code": "ATND",
                                "display": "attender"
                            }
                        ]
                    }
                ],
                "individual": {
                    "reference": f"Practitioner/{dr_id}",
                    "display": dr_name
                }
            },
            {
                "type": [
                    {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/v3-ParticipationType",
                                "code": "ADM",
                                "display": "admitter"
                            }
                        ]
                    }
                ],
                "individual": {
                    "reference": f"Practitioner/{dr_id}",
                    "display": dr_name
                }
            }
        ],
        "period": {
            "start": period_start
        },
        "reasonCode": [
            {
                "coding": [
                    {
                        "system": "http://snomed.info/sct",
                        "code": snomed,
                        "display": display
                    }
                ],
                "text": text
            }
        ],
        "diagnosis": [
            {
                "condition": {
                    "display": text
                },
                "use": {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/diagnosis-role",
                            "code": "AD",
                            "display": "Admission diagnosis"
                        }
                    ]
                },
                "rank": 1
            }
        ],
        "location": [
            {
                "location": {
                    "reference": f"Location/{location['id']}",
                    "display": f"{unit_name} - Room {location['room']}, Bed {location['bed']}"
                },
                "status": "active",
                "physicalType": {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/location-physical-type",
                            "code": "bd",
                            "display": "Bed"
                        }
                    ]
                }
            }
        ],
        "serviceProvider": {
            "reference": f"Organization/{hosp_id}",
            "display": hosp_name
        }
    }
    
    if period_end:
        encounter["period"]["end"] = period_end
    
    if discharge_disposition:
        disp_code, disp_display, disp_text = discharge_disposition
        encounter["hospitalization"] = {
            "admitSource": {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/admit-source",
                        "code": random.choice(["emd", "outp", "born", "gp", "hosp-trans"]),
                        "display": "From ED"
                    }
                ]
            },
            "dischargeDisposition": {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/discharge-disposition",
                        "code": disp_code,
                        "display": disp_display
                    }
                ],
                "text": disp_text
            }
        }
    
    return encounter


def generate_location(location_id, unit_info, room, bed, hospital):
    """Generate a FHIR Location resource."""
    unit_code, unit_name, unit_type = unit_info
    hosp_id, hosp_name, hosp_city, hosp_state, hosp_addr = hospital
    
    return {
        "resourceType": "Location",
        "id": location_id,
        "meta": {
            "lastUpdated": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        },
        "identifier": [
            {
                "system": "http://hospital.example.org/location-id",
                "value": f"LOC-{unit_code}-{room}-{bed}"
            }
        ],
        "status": "active",
        "name": f"{unit_name} - Room {room}, Bed {bed}",
        "description": f"Bed {bed} in Room {room} of {unit_name}",
        "mode": "instance",
        "type": [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
                        "code": {"icu": "ICU", "ward": "HU", "emergency": "ER", "bu": "HU", "or": "OR"}.get(unit_type, "HU"),
                        "display": unit_name
                    }
                ]
            }
        ],
        "physicalType": {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/location-physical-type",
                    "code": "bd",
                    "display": "Bed"
                }
            ]
        },
        "partOf": {
            "reference": f"Organization/{hosp_id}",
            "display": hosp_name
        },
        "managingOrganization": {
            "reference": f"Organization/{hosp_id}",
            "display": hosp_name
        }
    }


def generate_adt_message(message_num, event_info, patient_id, patient_name, patient_resource,
                         encounter_id, encounter_resource, location_resource, hospital, timestamp):
    """Generate a single ADT message bundle."""
    event_code, event_display, event_type = event_info
    message_id = f"msg-{patient_id}-{message_num:03d}"
    
    message_header = generate_message_header(
        message_id, event_code, event_display, event_type,
        timestamp, patient_id, encounter_id, hospital
    )
    
    return {
        "resourceType": "Bundle",
        "id": f"adt-{message_id}",
        "type": "message",
        "timestamp": timestamp,
        "entry": [
            {
                "fullUrl": f"urn:uuid:{generate_uuid()}",
                "resource": message_header
            },
            {
                "fullUrl": f"urn:uuid:patient-{patient_id}",
                "resource": patient_resource
            },
            {
                "fullUrl": f"urn:uuid:encounter-{encounter_id}",
                "resource": encounter_resource
            },
            {
                "fullUrl": f"urn:uuid:location-{location_resource['id']}",
                "resource": location_resource
            }
        ]
    }


def generate_patient_adt_bundle(patient_num):
    """Generate complete ADT bundle with multiple messages for a single patient."""
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
    
    # Generate patient resource
    patient_resource = generate_patient(patient_id, first_name, last_name, gender, birth_date, city_info)
    
    # Select a hospital for this patient
    hospital = random.choice(HOSPITALS)
    
    # Generate 2-4 admission episodes
    num_episodes = random.randint(2, 4)
    all_messages = []
    
    base_time = datetime.now() - timedelta(days=random.randint(365, 730))
    
    for episode in range(num_episodes):
        encounter_id = f"{patient_id}-{episode+1:02d}"
        admission_reason = random.choice(ADMISSION_REASONS)
        attending = random.choice(ATTENDING_PHYSICIANS)
        
        # Track time through this episode
        episode_start = base_time + timedelta(days=random.randint(30, 180) * episode)
        current_time = episode_start
        
        # Generate locations for this episode
        locations_used = []
        message_num = len(all_messages) + 1
        
        # A04 - Register Patient (sometimes)
        if random.random() < 0.3:
            unit = ("ED", "Emergency Department", "emergency")
            room = str(random.randint(1, 30))
            bed = random.choice(BED_TYPES)
            loc_id = f"loc-{patient_id}-{episode+1:02d}-reg"
            
            location = {
                "id": loc_id,
                "unit": unit,
                "room": room,
                "bed": bed
            }
            
            location_resource = generate_location(loc_id, unit, room, bed, hospital)
            
            encounter_resource = generate_encounter(
                encounter_id, patient_id, patient_name, "EMER", "planned",
                admission_reason, location, current_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                None, attending, hospital
            )
            
            msg = generate_adt_message(
                message_num, ("A04", "ADT/ACK - Register a Patient", "register"),
                patient_id, patient_name, patient_resource,
                encounter_id, encounter_resource, location_resource, hospital,
                current_time.strftime("%Y-%m-%dT%H:%M:%SZ")
            )
            all_messages.append(msg)
            message_num += 1
            current_time += timedelta(minutes=random.randint(30, 120))
        
        # A01 - Admit Patient
        initial_unit = random.choice([u for u in HOSPITAL_UNITS if u[2] in ["emergency", "ward", "icu"]])
        room = str(random.randint(100, 500))
        bed = random.choice(BED_TYPES)
        loc_id = f"loc-{patient_id}-{episode+1:02d}-01"
        
        location = {
            "id": loc_id,
            "unit": initial_unit,
            "room": room,
            "bed": bed
        }
        locations_used.append(location)
        
        location_resource = generate_location(loc_id, initial_unit, room, bed, hospital)
        
        admit_time = current_time
        encounter_resource = generate_encounter(
            encounter_id, patient_id, patient_name, "IMP", "in-progress",
            admission_reason, location, admit_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            None, attending, hospital
        )
        
        msg = generate_adt_message(
            message_num, ("A01", "ADT/ACK - Admit/Visit Notification", "admit"),
            patient_id, patient_name, patient_resource,
            encounter_id, encounter_resource, location_resource, hospital,
            admit_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        )
        all_messages.append(msg)
        message_num += 1
        current_time += timedelta(hours=random.randint(4, 48))
        
        # A02 - Transfer Patient (1-3 transfers)
        num_transfers = random.randint(1, 3)
        for t in range(num_transfers):
            transfer_unit = random.choice([u for u in HOSPITAL_UNITS if u != initial_unit])
            room = str(random.randint(100, 500))
            bed = random.choice(BED_TYPES)
            loc_id = f"loc-{patient_id}-{episode+1:02d}-t{t+1:02d}"
            
            location = {
                "id": loc_id,
                "unit": transfer_unit,
                "room": room,
                "bed": bed
            }
            locations_used.append(location)
            
            location_resource = generate_location(loc_id, transfer_unit, room, bed, hospital)
            
            transfer_time = current_time
            encounter_resource = generate_encounter(
                encounter_id, patient_id, patient_name, "IMP", "in-progress",
                admission_reason, location, admit_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                None, attending, hospital
            )
            
            msg = generate_adt_message(
                message_num, ("A02", "ADT/ACK - Transfer a Patient", "transfer"),
                patient_id, patient_name, patient_resource,
                encounter_id, encounter_resource, location_resource, hospital,
                transfer_time.strftime("%Y-%m-%dT%H:%M:%SZ")
            )
            all_messages.append(msg)
            message_num += 1
            current_time += timedelta(hours=random.randint(12, 72))
            initial_unit = transfer_unit
        
        # A08 - Update Patient Info (sometimes)
        if random.random() < 0.4:
            update_time = current_time
            patient_resource_updated = patient_resource.copy()
            patient_resource_updated["meta"]["lastUpdated"] = update_time.strftime("%Y-%m-%dT%H:%M:%SZ")
            
            last_location = locations_used[-1]
            loc_id = last_location["id"]
            location_resource = generate_location(loc_id, last_location["unit"], last_location["room"], last_location["bed"], hospital)
            
            encounter_resource = generate_encounter(
                encounter_id, patient_id, patient_name, "IMP", "in-progress",
                admission_reason, last_location, admit_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                None, attending, hospital
            )
            
            msg = generate_adt_message(
                message_num, ("A08", "ADT/ACK - Update Patient Information", "update"),
                patient_id, patient_name, patient_resource_updated,
                encounter_id, encounter_resource, location_resource, hospital,
                update_time.strftime("%Y-%m-%dT%H:%M:%SZ")
            )
            all_messages.append(msg)
            message_num += 1
            current_time += timedelta(hours=random.randint(1, 12))
        
        # A03 - Discharge Patient
        discharge_time = current_time
        discharge_disp = random.choice(DISCHARGE_DISPOSITIONS[:3])  # Most common dispositions
        
        last_location = locations_used[-1]
        loc_id = last_location["id"]
        location_resource = generate_location(loc_id, last_location["unit"], last_location["room"], last_location["bed"], hospital)
        
        encounter_resource = generate_encounter(
            encounter_id, patient_id, patient_name, "IMP", "finished",
            admission_reason, last_location, admit_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            discharge_time.strftime("%Y-%m-%dT%H:%M:%SZ"), attending, hospital,
            discharge_disp
        )
        
        msg = generate_adt_message(
            message_num, ("A03", "ADT/ACK - Discharge/End Visit", "discharge"),
            patient_id, patient_name, patient_resource,
            encounter_id, encounter_resource, location_resource, hospital,
            discharge_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        )
        all_messages.append(msg)
        
        base_time = current_time + timedelta(days=random.randint(30, 90))
    
    # Create wrapper bundle containing all ADT messages
    wrapper_bundle = {
        "resourceType": "Bundle",
        "id": f"adt-patient-bundle-{patient_id}",
        "type": "collection",
        "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "meta": {
            "profile": ["http://hl7.org/fhir/us/core/StructureDefinition/us-core-bundle"]
        },
        "entry": [
            {
                "fullUrl": f"urn:uuid:adt-msg-{i+1}",
                "resource": msg
            }
            for i, msg in enumerate(all_messages)
        ]
    }
    
    return wrapper_bundle, patient_name


def main():
    """Generate 100 ADT patient bundles."""
    output_dir = Path(__file__).parent / "data"
    output_dir.mkdir(exist_ok=True)
    
    # Remove old bundle files
    for f in output_dir.glob("*.json"):
        f.unlink()
    
    print("Generating 100 FHIR ADT patient bundles...")
    
    generated_names = set()
    
    for i in range(1, 101):
        bundle, patient_name = generate_patient_adt_bundle(i)
        
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
        
        num_messages = len(bundle["entry"])
        print(f"  [{i:3d}/100] {filepath.name} - {num_messages} ADT messages")
    
    print(f"\nDone! Generated 100 ADT bundles in {output_dir}")


if __name__ == "__main__":
    main()

