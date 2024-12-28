FROM python:3.12-slim


WORKDIR /

COPY . .


RUN pip install -r requirements.txt



# Copy the main.py file and Dockerfile (if needed) into the container


EXPOSE 8000

CMD ["uvicorn", "main:app","--host", "0.0.0.0", "--port", "8000"]