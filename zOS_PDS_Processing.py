# sample Python program showing how to access z/OS PDSes from OMVS. This
# program searches for a member in a concatenation of PDSes and prints the
# first one it finds.

import os

def find_and_echo_member(member_name, pds_libraries):
    """
    Simulates a SYSLIB search.
    Searches through a list of PDS libraries for a specific member.
    Processes and prints the first match found.
    """
    found = False

    for pds in pds_libraries:
        # On z/OS Python, datasets are accessed using the //'DATASET.NAME(MEMBER)' syntax.
        # We wrap the name in single quotes to ensure it is treated as a fully qualified name.
        pds_path = f"//'{pds}({member_name})'"
        
        # Check if the PDS member exists
        if os.path.exists(pds_path):
            print(f"--- Found {member_name} in {pds} ---")
            try:
                # Open the member for reading. The IBM Open Enterprise SDK for Python
                # handles the EBCDIC to ASCII/UTF-8 translation automatically for 
                # text-based files when running in the OMVS shell.
                with open(pds_path, 'r') as member_file:
                    for line in member_file:
                        print(line.rstrip())
                
                found = True
                break  # Exit after processing the first one found (SYSLIB behavior)
            
            except Exception as e:
                print(f"Error reading member: {e}")
                break
    
    if not found:
        print(f"Member {member_name} not found in provided libraries.")

if __name__ == "__main__":
    # Example usage:
    # member_to_find = "MYCOPY"
    # my_syslib = ["PROD.COPYLIB", "STAGE.COPYLIB", "USER.COPYLIB"]
    
    target_member = "MYMEMBER"
    search_order = [
        "USER.TEST.PDS",
        "SYS1.PROCLIB",
        "APP.SOURCE.LIB"
    ]
    
    find_and_echo_member(target_member, search_order)