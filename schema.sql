-- User Table
CREATE TABLE User (
    uid INT,
    email TEXT NOT NULL,
    username TEXT NOT NULL,
    PRIMARY KEY (uid)
);

-- Agent Creator Table
CREATE TABLE AgentCreator (
    uid INT,
    bio TEXT,
    payout TEXT,
    PRIMARY KEY (uid),
    FOREIGN KEY (uid) REFERENCES User(uid) ON DELETE CASCADE
);

-- Agent Client Table
CREATE TABLE AgentClient (
    uid INT,
    interests TEXT NOT NULL,
    cardholder TEXT NOT NULL,
    expire DATE NOT NULL,
    cardno BIGINT NOT NULL,
    cvv INT NOT NULL,
    zip INT NOT NULL,
    PRIMARY KEY (uid),
    FOREIGN KEY (uid) REFERENCES User(uid) ON DELETE CASCADE
);

-- Base Model Table
CREATE TABLE BaseModel (
    bmid INT,
    creator_uid INT NOT NULL,
    description TEXT NOT NULL,
    PRIMARY KEY (bmid),
    FOREIGN KEY (creator_uid) REFERENCES AgentCreator(uid) ON DELETE CASCADE
);

-- Customized Model Table (Weak Entity)
CREATE TABLE CustomizedModel (
    bmid INT,
    mid INT,
    PRIMARY KEY (bmid, mid),
    FOREIGN KEY (bmid) REFERENCES BaseModel(bmid) ON DELETE CASCADE
);

-- Configuration Table
CREATE TABLE Configuration (
    cid INT,
    client_uid INT NOT NULL,
    content TEXT NOT NULL,
    labels TEXT NOT NULL,
    PRIMARY KEY (cid),
    FOREIGN KEY (client_uid) REFERENCES AgentClient(uid) ON DELETE CASCADE
);

-- Internet Service Table
CREATE TABLE InternetService (
    sid INT,
    provider TEXT NOT NULL,
    endpoints TEXT NOT NULL,
    PRIMARY KEY (sid)
);

-- LLM Table
CREATE TABLE LLMService (
    sid INT,
    domain TEXT,
    PRIMARY KEY (sid),
    FOREIGN KEY (sid) REFERENCES InternetService(sid) ON DELETE CASCADE
);

-- Data Storage Table
CREATE TABLE DataStorage (
    sid INT,
    type TEXT,
    PRIMARY KEY (sid),
    FOREIGN KEY (sid) REFERENCES InternetService(sid) ON DELETE CASCADE
);

-- Model x Services Table
CREATE TABLE ModelServices (
    bmid INT NOT NULL,
    sid INT NOT NULL,
    version INT NOT NULL,
    PRIMARY KEY (bmid, sid),
    FOREIGN KEY (bmid) REFERENCES BaseModel(bmid) ON DELETE CASCADE,
    FOREIGN KEY (sid) REFERENCES InternetService(sid) ON DELETE CASCADE
);

-- Model x Configurations Table
CREATE TABLE ModelConfigurations (
    bmid INT NOT NULL,
    mid INT NOT NULL,
    cid INT NOT NULL,
    duration INT NOT NULL,
    PRIMARY KEY (bmid, mid, cid),
    FOREIGN KEY (bmid, mid) REFERENCES CustomizedModel(bmid, mid) ON DELETE CASCADE,
    FOREIGN KEY (cid) REFERENCES Configuration(cid) ON DELETE CASCADE
);
