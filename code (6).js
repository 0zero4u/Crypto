function handleAggTradeMessage(jsonDataString) {
    try {
        const trade = JSON.parse(jsonDataString); // Standard JSON parsing for aggTrade
        // ... rest of the logic
    } catch (error) {
        // This catch block is for errors during JSON.parse or processing of the parsed trade object.
        console.error('[Listener] Error in handleAggTradeMessage (JSON.parse or processing failed):', error.message, error.stack);
        console.error('[Listener] Offending aggTrade data string for handleAggTradeMessage:', jsonDataString.substring(0, 250));
    }
}