
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI()

# Sample data
users = [
    {"id": 1, "name": "John"},
    {"id": 2, "name": "Alice"}
]

# GET endpoint
@app.get("/")
def home():
    return {"message": "Welcome to Python API"}

# GET all users
@app.get("/users")
def get_users():
    return users

# GET user by ID
@app.get("/users/{user_id}")
def get_user(user_id: int):
    for user in users:
        if user["id"] == user_id:
            return user
    raise HTTPException(status_code=404, detail="User not found")

# POST request model
class User(BaseModel):
    id: int
    name: str

# POST endpoint
@app.post("/users")
def create_user(user: User):
    users.append(user.model_dump())
    return {
        "message": "User created successfully",
        "user": user
    }

# DELETE endpoint
@app.delete("/users/{user_id}")
def delete_user(user_id: int):
    for user in users:
        if user["id"] == user_id:
            users.remove(user)
            return {"message": "User deleted"}
    raise HTTPException(status_code=404, detail="User not found")
