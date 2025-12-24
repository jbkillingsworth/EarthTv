import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom'

async function fetchData(longitude, latitude) {
  try {
    // Await the Promise returned by fetch().
    // The function pauses here until the response is available.
    const response = await fetch('http://localhost:8080/create-video-request?longitude=' + longitude +
    '&latitude=' + latitude + '&collection=1');

    if (!response.ok) {
      throw new Error(`HTTP error: ${response.status}`);
    }

    // Await the Promise returned by response.json().
    // The function pauses again until the JSON is parsed.
    const data = await response.json();

    return data;

  } catch (error) {
    // Catches any errors thrown by the awaits
    console.error('Could not fetch data:', error);
  }
}

export default function Home() {
  // 1. Initialize state for username and password
  const [longitude, setLongitude] = useState('');
  const [latitude, setLatitude] = useState('');

  // 2. Handle input changes
  const handleLongitudeChange = (event) => {
    setLongitude(event.target.value);
  };

  const handleLatitudeChange = (event) => {
    setLatitude(event.target.value);
  };

  const navigate = useNavigate();

  // 3. Handle form submission
  const handleSubmit = (event) => {
    event.preventDefault(); // Prevents the default browser page reload
    try {
        fetchData(longitude, latitude).then(data => {
            navigate('/videos/'+data.id);
            console.log(data.id)
        })
    } catch (error) {
      // Handle network errors or exceptions
      console.error('Network error:', error);
      navigate('/error'); // Redirect for critical errors
    }
  };

  // 4. Render the form
  return (
    <form onSubmit={handleSubmit}>
      <div>
        <label htmlFor="longitude">Longitude:</label>
        <input
          type="text"
          id="longitude"
          value={longitude} // The value is controlled by the state
          onChange={handleLongitudeChange} // The state updates on change
        />
      </div>
      <div>
        <label htmlFor="latitude">Latitude:</label>
        <input
          type="text"
          id="latitude"
          value={latitude} // The value is controlled by the state
          onChange={handleLatitudeChange} // The state updates on change
        />
      </div>
      <button type="submit">Submit</button>
    </form>
  );
}