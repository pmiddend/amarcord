import bcrypt

pw = input("Enter password (caution, this is output here!): ")

salt = bcrypt.gensalt()
hashed_password = bcrypt.hashpw(pw.encode("utf-8"), salt).decode("utf-8")

print("Password hash is:")
print(hashed_password)
