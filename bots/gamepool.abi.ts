// Minimal, stable ABI for the bot (reads + sendRequest/retryRequest).
// If you want the full ABI, you can paste more entries — but you only need these.
export const gamePoolAbi = [
  // --- reads used by the bot gates ---
  { type:"function", stateMutability:"view", name:"league",      inputs:[], outputs:[{type:"string"}] },
  { type:"function", stateMutability:"view", name:"teamAName",   inputs:[], outputs:[{type:"string"}] },
  { type:"function", stateMutability:"view", name:"teamBName",   inputs:[], outputs:[{type:"string"}] },
  { type:"function", stateMutability:"view", name:"teamACode",   inputs:[], outputs:[{type:"string"}] },
  { type:"function", stateMutability:"view", name:"teamBCode",   inputs:[], outputs:[{type:"string"}] },
  { type:"function", stateMutability:"view", name:"isLocked",    inputs:[], outputs:[{type:"bool"}] },
  { type:"function", stateMutability:"view", name:"requestSent", inputs:[], outputs:[{type:"bool"}] },
  { type:"function", stateMutability:"view", name:"winningTeam", inputs:[], outputs:[{type:"uint8"}] },
  { type:"function", stateMutability:"view", name:"lockTime",    inputs:[], outputs:[{type:"uint256"}] },
  { type:"function", stateMutability:"view", name:"owner",       inputs:[], outputs:[{type:"address"}] },

  // --- writes the bot may call ---
  {
    type:"function",
    stateMutability:"nonpayable",
    name:"sendRequest",
    inputs:[
      {type:"string"},      // source
      {type:"string[]"},    // args (length must be 8)
      {type:"uint64"},      // subscriptionId
      {type:"uint32"},      // gasLimit
      {type:"uint8"},       // donHostedSecretsSlotID
      {type:"uint64"},      // donHostedSecretsVersion
      {type:"bytes32"},     // donID
    ],
    outputs:[]
  },
  {
    type:"function",
    stateMutability:"nonpayable",
    name:"retryRequest",
    inputs:[
      {type:"string"},
      {type:"string[]"},
      {type:"uint64"},
      {type:"uint32"},
      {type:"uint8"},
      {type:"uint64"},
      {type:"bytes32"},
    ],
    outputs:[]
  }
] as const;
