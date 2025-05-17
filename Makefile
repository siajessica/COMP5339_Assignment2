.PHONY: broker publisher subcriber clean used kill

broker:
	mosquitto -v

publisher:
	python simple_retrieve_publish.py

app:
	streamlit run app.py

clean:
	rm -f *.csv
	rm -f *.json

used:
	sudo lsof -i :1883

kill:
	sudo kill $(PID)