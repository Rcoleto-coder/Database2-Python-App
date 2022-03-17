from operator import truediv
import ui
import db
import os
import hashlib

def add_person():
    responses = ui.dialog("Add Person", (
        "First Name",
        "Last Name",
        ("Birthday (yyyy-mm-dd)", "^\d\d\d\d-\d\d-\d\d$|^$"),
        # NOTE: This regex does not truly match valid email addresses
        # See https://stackoverflow.com/questions/201323/how-can-i-validate-an-email-address-using-a-regular-expression
        ("Email: ", ".*@.*|^$"),
        "Address line 1",
        "Address line 2",
        "City",
        "Province/State",
        "Country",
        "Post code"
    ))

    fields = ('first_name', 'last_name', 'birthday', 'email', 
                        'address_line1', 'address_line2','city','prov','country','postcode')
    data = dict(zip(fields, responses))

    numbers = []
    while "y" == ui.constrained_input("Add a phone number (y/n)? ", ['y','n'], "I don't understand"):
        label = ui.options("What kind of phone number?", (('CELL', 'Cell'), ('HOME', 'Home'), ('WORK', 'Work'), ('OTHER', 'Other')))
        number = ui.regex_input("Phone number (###-###-####)", "^\d\d\d-\d\d\d-\d\d\d\d$")
        numbers.append({'number':number, 'label':label})

    data['phone_numbers'] = numbers

    print(data)

    db.add_person(data)

def delete_person():
    ids = db.get_person_ids()
    id = int(ui.constrained_input("ID of person to delete: ", ids, "There is no person with that ID" ))

    db.delete_person(id)

def list_people():
    print()
    selection = ui.options("Sort by...", list(zip(db.PERSON_SORTABLE_FIELDS, db.PERSON_SORTABLE_FIELD_HEADINGS)))
    people = db.get_people_list(order_by=selection)

    def format_address(person):
        """Formats the given person's address info into a single string, and returns that string."""

        address = person['address_line1'] + "\n" 

        if ( person['address_line2'] ):
            address += person['address_line2'] + "\n" 
        
        address += person['city'] + ", " + person['prov'] + ", " + person['country'] + "\n" 
        address += person['postcode']

        return address

    def format_phone_info(person):
        """Formats the phone numbers of a person into a single string.
            persons must contain a 'phone_numbers' key that has as its value a list of
            { 'number', 'label' } dicts.
            The return string will have each number on its own line, prefixed with one of
            (W), (H), (C), or (O) (for the labels work, home, cell, or other)"""

        if not ('phone_numbers' in person):
            return None

        numbers = []
        for ph in person['phone_numbers']:
            if ph['number'] is None:
                numbers = []
            else:
                numbers.append(f"({ph['label'][0]}) {ph['number']}")
            
        return "\n".join(numbers)

    # Massage the data we get back from the data layer into the form expected by the presentation layer
    data = []
    for p in people:
        data.append((
            p['person_id'],
            p['first_name'],
            p['last_name'],
            p['birthday'],
            p['email'],
            format_phone_info(p),
            format_address(p)
        ))
    
    print()
    ui.print_heading("People List")
    print()
    ui.table(('ID','First Name','Last Name','Birthday','Email','Phone','Address'), data)
    
def change_password():
    """Change the password for the currently logged in user."""
    username = user['username']    # The user object is set upon login; get their username
    new = ui.new_password_input()  # Prompt the user for a new password

    # TODO: Store the password SECURELY in the database - COMPLETED
    hash_name = 'sha256'
    salt = os.urandom(20) # creates 20 random bytes for use as a salt
    iterations = 500000
    encrypted_password = hashlib.pbkdf2_hmac(hash_name, new.encode('utf-8'), salt, iterations)
    password_hash = f"{hash_name}${iterations}${salt.hex()}${encrypted_password.hex()}"

    db.update_password(username,password_hash)


def login():
    """Prompts the user for their username and password.
        If the user username and password are correct, returns the user information obtained
        from the database; otherwise it returns null"""
    (username, password) = ui.login_input()

    # TODO: Check if the username/password combination is correct
    #       If it is, return the user information from the database; otherwise return None
    # COMPLETED
    
    user, password_hash = db.get_user(username).values()
    hash_name, iterations, salt, encrypted_password = password_hash.split('$')
    key = hashlib.pbkdf2_hmac(hash_name, password.encode('utf-8'), bytes.fromhex(salt), int(iterations))
   
    if username == user and key.hex() == encrypted_password:
        return db.get_user(username)
    else:
        return None
        

def quit():
    exit(0)

user = login()
if ( user ):
    while True:
        ui.menu("Main Menu", (
            ("_List people", list_people),
            ("_Add person", add_person),
            ("_Delete person", delete_person),
            ("_Change password", change_password),
            ("_Quit", quit)
        ))
else:
    print("Invalid credentials")
