import React, { useState, useEffect } from 'react';
import { useParams } from 'react-router-dom';
import { BrowserRouter, Routes, Route, Link } from 'react-router-dom';

const Video = () => {
  const { videoId } = useParams(); // Get userId from the URL (e.g., /user/123)
  const [videoData, setVideoData] = useState(null);

  useEffect(() => {
    // Fetch user data based on userId from an API
//    fetch(`http://localhost:8080/create-video-request?longitude=-132&latitude=41&collection=1`)
    const url = 'http://localhost:8080/get-video-status?id=' + videoId;
    fetch(url)
      .then(response => response.json())
      .then(data => setVideoData(data));
  }, [videoId]); // Re-run effect if userId changes

  if (!videoData) {
    return <div>Loading...</div>;
  }

  console.log(videoData)

  if (videoData.status == 0) {
    return <div>Request Queued. Video is Processing...</div>;
  }

  return (
    <div>
      <h1>Video ID: {videoData.id}</h1>
      <p>Longitude: {videoData.longitude}</p>
      <p>Latitude: {videoData.latitude}</p>
    </div>
  );
};

export default Video;