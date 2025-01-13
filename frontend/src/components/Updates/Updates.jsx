// Updates.jsx
import React, { useEffect, useState } from "react";
import "./Updates.css";
import { fetchLastComments } from "../../Data/Data"; // Import the new function

const Updates = ({ companyName }) => {
  const [lastComments, setLastComments] = useState({
    reddit: "",
    trustpilot: "",
    youtube: "",
  });

  useEffect(() => {
    const loadLastComments = async () => {
      try {
        const data = await fetchLastComments(companyName);
        setLastComments(data);
      } catch (error) {
        console.error("Error loading last comments:", error);
      }
    };

    loadLastComments();
  }, [companyName]);

  return (
    <div className="Updates">
      <h3>Latest Comments</h3>
      
      {/* Reddit */}
      <div className="update">
        <img src="reddit-icon.png" alt="Reddit" />
        <div className="noti">
          <div style={{ marginBottom: "0.5rem" }}>
            <span>Reddit</span>
          </div>
          <span>{lastComments.reddit || "No comment yet"}</span>
        </div>
      </div>

      {/* Trustpilot */}
      <div className="update">
        <img src="trustpilot-icon.png" alt="Trustpilot" />
        <div className="noti">
          <div style={{ marginBottom: "0.5rem" }}>
            <span>Trustpilot</span>
          </div>
          <span>{lastComments.trustpilot || "No comment yet"}</span>
        </div>
      </div>

      {/* YouTube */}
      <div className="update">
        <img src="youtube-icon.png" alt="YouTube" />
        <div className="noti">
          <div style={{ marginBottom: "0.5rem" }}>
            <span>YouTube</span>
          </div>
          <span>{lastComments.youtube || "No comment yet"}</span>
        </div>
      </div>
    </div>
  );
};

export default Updates;
