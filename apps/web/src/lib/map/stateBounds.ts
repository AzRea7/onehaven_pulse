export type StateOption = {
  value: string;
  label: string;
  bounds: [[number, number], [number, number]];
  center: [number, number];
  zoom: number;
};

export const STATE_OPTIONS: StateOption[] = [
  {
    value: "",
    label: "Whole USA",
    bounds: [
      [24.396308, -124.848974],
      [49.384358, -66.885444],
    ],
    center: [39.8283, -98.5795],
    zoom: 4,
  },
  {
    value: "MI",
    label: "Michigan",
    bounds: [
      [41.6961, -90.4184],
      [48.3061, -82.1228],
    ],
    center: [44.3148, -85.6024],
    zoom: 6,
  },
  {
    value: "OH",
    label: "Ohio",
    bounds: [
      [38.4032, -84.8203],
      [41.9773, -80.5187],
    ],
    center: [40.4173, -82.9071],
    zoom: 7,
  },
  {
    value: "IL",
    label: "Illinois",
    bounds: [
      [36.9701, -91.5131],
      [42.5083, -87.0199],
    ],
    center: [40.6331, -89.3985],
    zoom: 6,
  },
  {
    value: "IN",
    label: "Indiana",
    bounds: [
      [37.7717, -88.0978],
      [41.7614, -84.7846],
    ],
    center: [40.2672, -86.1349],
    zoom: 7,
  },
  {
    value: "PA",
    label: "Pennsylvania",
    bounds: [
      [39.7198, -80.5199],
      [42.2699, -74.6895],
    ],
    center: [41.2033, -77.1945],
    zoom: 7,
  },
  {
    value: "TX",
    label: "Texas",
    bounds: [
      [25.8371, -106.6456],
      [36.5007, -93.5083],
    ],
    center: [31.9686, -99.9018],
    zoom: 5,
  },
  {
    value: "FL",
    label: "Florida",
    bounds: [
      [24.3963, -87.6349],
      [31.0009, -80.0314],
    ],
    center: [27.6648, -81.5158],
    zoom: 6,
  },
  {
    value: "GA",
    label: "Georgia",
    bounds: [
      [30.3556, -85.6052],
      [35.0007, -80.7514],
    ],
    center: [32.1656, -82.9001],
    zoom: 7,
  },
  {
    value: "AZ",
    label: "Arizona",
    bounds: [
      [31.3322, -114.8184],
      [37.0043, -109.0452],
    ],
    center: [34.0489, -111.0937],
    zoom: 6,
  },
  {
    value: "CA",
    label: "California",
    bounds: [
      [32.5343, -124.4096],
      [42.0095, -114.1312],
    ],
    center: [36.7783, -119.4179],
    zoom: 5,
  },
  {
    value: "WA",
    label: "Washington",
    bounds: [
      [45.5435, -124.8489],
      [49.0024, -116.9161],
    ],
    center: [47.7511, -120.7401],
    zoom: 7,
  },
  {
    value: "NY",
    label: "New York",
    bounds: [
      [40.4774, -79.7624],
      [45.0159, -71.7517],
    ],
    center: [43.2994, -74.2179],
    zoom: 6,
  },
  {
    value: "NC",
    label: "North Carolina",
    bounds: [
      [33.7529, -84.3219],
      [36.5881, -75.4001],
    ],
    center: [35.7596, -79.0193],
    zoom: 7,
  },
  {
    value: "TN",
    label: "Tennessee",
    bounds: [
      [34.9829, -90.3103],
      [36.6781, -81.6469],
    ],
    center: [35.5175, -86.5804],
    zoom: 7,
  },
];

export function getStateOption(value: string): StateOption {
  return STATE_OPTIONS.find((option) => option.value === value) ?? STATE_OPTIONS[0];
}
