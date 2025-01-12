import React from "react";
import Updates from "../Updates/Updates";
import "./RightSide.css";
import Interactions from "../Interactions/Interactions";

const RightSide = () => {
  return (
    <div className="RightSide">
      <div>
        <h3>Updates</h3>
        <Updates />
      </div>
      <div>
        <h3>Interaction Frequency in the last 30 days</h3>
        <Interactions />
      </div>
    </div>
  );
};

export default RightSide;
