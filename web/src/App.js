import { BrowserRouter, Routes, Route, Link } from 'react-router-dom';
import Home from './pages/Home';
import About from './pages/About';
import Video from './pages/Video';

function App() {
  return (
    <BrowserRouter>
      <nav>
        <Link to="/">Home</Link> | {" "}
        <Link to="/about">About</Link>
      </nav>
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="about" element={<About />} />
        <Route path="/videos/:videoId" element={<Video />} />
      </Routes>
    </BrowserRouter>
  );
}

export default App;