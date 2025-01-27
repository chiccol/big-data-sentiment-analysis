import React from "react";
import Updates from "../Updates/Updates";
import "./RightSide.css";
import Interactions from "../Interactions/Interactions";

const RightSide = ({ company }) => {
  return (
    <div className="RightSide">
      <div>
        <h3>Updates</h3>
        <Updates companyName={company}/>
      </div>
      <div>
        <h3>Interaction Frequency in the last 30 days</h3>
        <Interactions company={company}/>
      </div>
    </div>
  );
};

export default RightSide;
