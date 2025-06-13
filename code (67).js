        // NEW, STATE-AWARE LOGIC
        const dynamicThreshold = SCORE_THRESHOLDS_BY_BIAS[lastSentPrediction.bias] || 1.5;
        const scoreChangedSignificantly = Math.abs(score - lastSentPrediction.score) > dynamicThreshold;
        const biasChanged = currentBias !== lastSentPrediction.bias;