from openai import OpenAI
client = OpenAI()

resp = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[
        {"role": "system", "content": "You are concise and precise, using British English."},
        {"role": "user", "content": "In one sentence, tell me you received my API call."}
    ],
    temperature=0
)
print(resp.choices[0].message.content)
