export const wispRates = { 
  "wisp_rates": [ // feel free to change rates 
    { 
      "rate": "test-rate", // rate limited to 50 test requests in 1 min 
      "request": 50,
      "time": 60000 
    }, 
    { 
      "rate": "main-rate", // leave at 0 for unlimited with rate throttling, both 0 is fully unlimited 
      "request": 50000,
      "time": 0 
    }
  ]
};
