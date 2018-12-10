# symphony-python-asynchronous
Implementation of Symphony REST API in Python using asynchronous calls for increased throughput
I started implementing Symphony's REST API in Python a year ago. Back then there was no Python API from Symphony.
I used requests module to make the calls. As the customer base increased I noticed sluggishness. 
The reason was 300ms/post call to send message. It was in a single process and so 100 messages took 30 sec - too laggy.
The way out of this was - multi-threading or redoing everything in nodejs. I did not like either option and luckily came across asyncio and aiohttp - Python libraries with asychronous event loop. This was perfect for my needs.
The lag went from 300ms to under 4 ms. 
Current symphony python api does not have this feature so hopefully this can help.

My first github commit so feedback is encouraged.


