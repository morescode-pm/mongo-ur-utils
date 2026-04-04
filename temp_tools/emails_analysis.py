import json
from collections import defaultdict

def analyze_emails(filename):
    # Dictionaries to store our findings
    capitalized_emails = set()
    email_records = defaultdict(list)  # Will store {lowercase_email: [(email, id, updatedAt), ...]}
    
    try:
        with open(filename, 'r') as file:
            data = json.load(file)
            
        for entry in data:
            # Skip entries without email
            if 'email' not in entry:
                continue
                
            email = entry['email']
            user_id = entry['_id']['$oid']
            updated_at = entry.get('updatedAt', {}).get('$date', 'No update date')
            
            # Check for capital letters
            if any(c.isupper() for c in email):
                capitalized_emails.add(email)
            
            # Track lowercase versions with their original form, ID and update time
            email_lower = email.lower()
            email_records[email_lower].append((email, user_id, updated_at))
        
        # Print report
        print("\n=== Emails with Capital Letters ===")
        if capitalized_emails:
            for email in sorted(capitalized_emails):
                print(email)
        else:
            print("No emails with capital letters found.")
            
        print("\n=== Duplicate Emails (when converted to lowercase) ===")
        duplicates = {k: v for k, v in email_records.items() if len(v) > 1}
        
        if duplicates:
            for lower_email, variations in duplicates.items():
                print(f"\nLowercase form: {lower_email}")
                print("Variations found:")
                for original_email, user_id, update_time in variations:
                    print(f"  - {original_email}")
                    print(f"    ID: {user_id}")
                    print(f"    Last updated: {update_time}")
        else:
            print("No duplicate emails found when converted to lowercase.")
            
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found.")
    except json.JSONDecodeError:
        print("Error: Invalid JSON file")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    filename = "test.users.json"
    analyze_emails(filename)