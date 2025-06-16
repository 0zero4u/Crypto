// Inside connectToBinanceStream()
binanceWsClient.on('message', (data) => {
    try {
        const eventData = JSON.parse(data.toString());
        
        // This condition is true for every book ticker update
        if (eventData.b && eventData.a) {
            
            // ... state is updated with the new quantities ...

            // THIS IS THE KEY: We call the calculation on every single message.
            calculateAndSendSignal();
            
            // ... 'last' state is updated for the next event ...
        }
    } // ...
});