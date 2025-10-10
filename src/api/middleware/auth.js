const protectWithToken = (req, res, next) => {
  const header = req.headers["authorization"];

  if (!header) {
    return res.status(401).json({ message: "Authorization header is missing" });
  }

  const token = header.split(" ")[1]; 

  if (token !== process.env.SECRET_REFRESH_TOKEN) {
    return res.status(403).json({ message: "Forbidden: Invalid token" });
  }

  next(); // Token is valid, proceed to the next function (the controller)
};

export { protectWithToken };
