-- ============================================================================
-- 反欺诈与反恐融资风险识别智能体系统 - 数据库初始化脚本
-- 数据库: risk_db
-- MySQL 8.0+
-- ============================================================================

CREATE DATABASE IF NOT EXISTS risk_db
    DEFAULT CHARACTER SET utf8mb4
    DEFAULT COLLATE utf8mb4_unicode_ci;

USE risk_db;

-- ============================================================================
-- 1. 规则定义表
-- ============================================================================
CREATE TABLE IF NOT EXISTS rule_definition (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    rule_id VARCHAR(32) NOT NULL UNIQUE COMMENT '规则编号: R001/T001',
    rule_name VARCHAR(128) NOT NULL COMMENT '规则名称',
    rule_category VARCHAR(32) NOT NULL COMMENT '规则分类: AMOUNT/TIME/REGION/DEVICE/BEHAVIOR/CTF',
    rule_expression TEXT COMMENT '规则表达式(DRL/JSON)',
    risk_weight INT NOT NULL DEFAULT 0 COMMENT '风险权重 0-100',
    risk_level VARCHAR(8) NOT NULL DEFAULT 'MEDIUM' COMMENT '风险等级: LOW/MEDIUM/HIGH',
    description VARCHAR(512) COMMENT '规则描述',
    conditions_json JSON COMMENT '触发条件JSON',
    enabled TINYINT(1) NOT NULL DEFAULT 1 COMMENT '是否启用',
    version INT NOT NULL DEFAULT 1 COMMENT '版本号',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_rule_enabled (enabled),
    INDEX idx_rule_category (rule_category)
) ENGINE=InnoDB COMMENT='规则定义表';

-- ============================================================================
-- 2. 黑名单表
-- ============================================================================
CREATE TABLE IF NOT EXISTS black_list (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    list_type VARCHAR(32) NOT NULL COMMENT '名单类型: SANCTION/INTERNAL/HIGH_RISK',
    entity_type VARCHAR(32) NOT NULL COMMENT '实体类型: ACCOUNT/NAME/ID_CARD/PHONE/IP',
    entity_value VARCHAR(256) NOT NULL COMMENT '实体值(脱敏)',
    entity_hash VARCHAR(64) NOT NULL COMMENT '实体SHA-256哈希',
    source VARCHAR(64) COMMENT '名单来源',
    risk_reason VARCHAR(512) COMMENT '风险原因',
    effective_time DATETIME NOT NULL COMMENT '生效时间',
    expire_time DATETIME COMMENT '过期时间',
    enabled TINYINT(1) NOT NULL DEFAULT 1,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_entity_hash (entity_hash),
    INDEX idx_list_type (list_type),
    INDEX idx_enabled_expire (enabled, expire_time)
) ENGINE=InnoDB COMMENT='黑名单表';

-- ============================================================================
-- 3. 交易记录表
-- ============================================================================
CREATE TABLE IF NOT EXISTS transaction_record (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    transaction_id VARCHAR(64) NOT NULL UNIQUE,
    account_id_hash VARCHAR(64) NOT NULL,
    counterparty_hash VARCHAR(64),
    transaction_type VARCHAR(16) NOT NULL,
    channel_code VARCHAR(16) NOT NULL,
    amount DECIMAL(18,2) NOT NULL,
    currency VARCHAR(3) NOT NULL DEFAULT 'CNY',
    transaction_time DATETIME NOT NULL,
    ip_address VARCHAR(45),
    ip_risk_level VARCHAR(16) DEFAULT 'NORMAL',
    device_id VARCHAR(64),
    is_emulator TINYINT(1) DEFAULT 0,
    is_rooted TINYINT(1) DEFAULT 0,
    is_cross_border TINYINT(1) DEFAULT 0,
    risk_score DECIMAL(5,2),
    risk_level VARCHAR(8),
    decision VARCHAR(16),
    processing_status VARCHAR(16) DEFAULT 'PENDING',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_account_hash (account_id_hash),
    INDEX idx_tx_time (transaction_time),
    INDEX idx_risk_level (risk_level)
) ENGINE=InnoDB COMMENT='交易记录表';

-- ============================================================================
-- 4. 欺诈案例表
-- ============================================================================
CREATE TABLE IF NOT EXISTS fraud_case (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    case_id VARCHAR(64) NOT NULL UNIQUE,
    case_title VARCHAR(256) NOT NULL,
    case_type VARCHAR(32) NOT NULL COMMENT 'FRAUD/AML/CTF',
    case_level VARCHAR(8) NOT NULL DEFAULT 'HIGH',
    case_description TEXT NOT NULL,
    modus_operandi TEXT COMMENT '作案手法',
    fraud_pattern VARCHAR(128) COMMENT '欺诈模式标签',
    key_features JSON COMMENT '关键特征',
    involved_amount DECIMAL(18,2) DEFAULT 0.00,
    resolution TEXT COMMENT '处置方案',
    resolution_effect VARCHAR(512) COMMENT '处置效果',
    case_status VARCHAR(16) DEFAULT 'OPEN',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_case_type (case_type),
    INDEX idx_fraud_pattern (fraud_pattern),
    FULLTEXT idx_fulltext (case_title, case_description, modus_operandi)
) ENGINE=InnoDB COMMENT='欺诈案例表';

-- ============================================================================
-- 5. 客户画像表
-- ============================================================================
CREATE TABLE IF NOT EXISTS customer_profile (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    customer_id VARCHAR(32) NOT NULL UNIQUE,
    customer_name_hash VARCHAR(64) NOT NULL,
    customer_type VARCHAR(16) NOT NULL DEFAULT 'PERSONAL',
    id_number_hash VARCHAR(64),
    phone_hash VARCHAR(64),
    risk_level_init VARCHAR(8) NOT NULL DEFAULT 'LOW',
    risk_level_current VARCHAR(8) NOT NULL DEFAULT 'LOW',
    customer_status VARCHAR(8) NOT NULL DEFAULT 'ACTIVE',
    open_date DATE,
    last_update DATETIME,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_risk_level (risk_level_current),
    INDEX idx_type (customer_type)
) ENGINE=InnoDB COMMENT='客户画像表';

-- ============================================================================
-- 6. 预警记录表
-- ============================================================================
CREATE TABLE IF NOT EXISTS alert_record (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    alert_id VARCHAR(64) NOT NULL UNIQUE,
    transaction_id VARCHAR(64) NOT NULL,
    customer_id VARCHAR(32),
    alert_type VARCHAR(16) NOT NULL DEFAULT 'FRAUD',
    risk_score DECIMAL(5,2) NOT NULL,
    risk_level VARCHAR(8) NOT NULL,
    agent_decision VARCHAR(16),
    matched_rules_json JSON,
    alert_status VARCHAR(16) NOT NULL DEFAULT 'NEW',
    assignee VARCHAR(32),
    escalation_level INT DEFAULT 0,
    processing_deadline DATETIME NOT NULL,
    processed_time DATETIME,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_alert_status (alert_status),
    INDEX idx_created_at (created_at),
    INDEX idx_transaction_id (transaction_id)
) ENGINE=InnoDB COMMENT='预警记录表';

-- ============================================================================
-- 7. 调查案例表
-- ============================================================================
CREATE TABLE IF NOT EXISTS investigation_case (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    case_no VARCHAR(64) NOT NULL UNIQUE,
    alert_id VARCHAR(64),
    case_title VARCHAR(256),
    case_type VARCHAR(32) NOT NULL DEFAULT 'FRAUD',
    customer_id VARCHAR(32),
    transaction_ids_json JSON,
    investigation_report TEXT,
    conclusion VARCHAR(512),
    case_status VARCHAR(16) NOT NULL DEFAULT 'DRAFT',
    investigator VARCHAR(32),
    reviewer VARCHAR(32),
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    reviewed_at DATETIME,
    INDEX idx_case_status (case_status),
    INDEX idx_alert_id (alert_id)
) ENGINE=InnoDB COMMENT='调查案例表';

-- ============================================================================
-- 8. 决策日志表 (审计追溯)
-- ============================================================================
CREATE TABLE IF NOT EXISTS decision_audit_log (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    transaction_id VARCHAR(64) NOT NULL,
    request_id VARCHAR(64),
    risk_score DECIMAL(5,2),
    rule_score DECIMAL(5,2),
    ml_score DECIMAL(5,2),
    kb_score DECIMAL(5,2),
    risk_level VARCHAR(8),
    decision VARCHAR(16),
    confidence DECIMAL(3,2),
    matched_rules JSON,
    similar_cases JSON,
    processing_time_ms INT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_tx_id (transaction_id),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB COMMENT='决策审计日志表';

-- ============================================================================
-- 插入初始测试数据
-- ============================================================================
INSERT INTO rule_definition (rule_id, rule_name, rule_category, risk_weight, risk_level, description) VALUES
('R001', '单笔大额交易', 'AMOUNT', 30, 'MEDIUM', '单笔交易金额≥50万元'),
('R002', '频繁大额交易', 'AMOUNT', 40, 'MEDIUM', '单日累计≥100万元'),
('R003', '分散转入集中转出', 'AMOUNT', 60, 'HIGH', '单日转入≥10笔且转出≥30万元'),
('R101', '凌晨高频交易', 'TIME', 40, 'MEDIUM', '00:00-05:00交易次数≥3笔且金额≥5万元'),
('R203', '高匿名IP交易', 'REGION', 50, 'HIGH', 'IP为VPN/代理/Tor出口节点'),
('R301', '新设备首次交易', 'DEVICE', 30, 'MEDIUM', '设备首次绑定且交易≥1万元'),
('R302', '模拟器检测', 'DEVICE', 60, 'HIGH', '设备为模拟器/虚拟机'),
('T001', '制裁名单命中', 'CTF', 100, 'HIGH', '交易对手命中制裁名单'),
('T002', '高风险行业交易', 'CTF', 55, 'HIGH', '交易对手为高风险行业'),
('T005', '涉及战乱国家', 'CTF', 75, 'HIGH', '交易对手位于受制裁/战乱国家');

INSERT INTO fraud_case (case_id, case_title, case_type, case_level, case_description, modus_operandi, fraud_pattern) VALUES
('CASE001', '境外信用卡盗刷团伙', 'FRAUD', 'HIGH', '犯罪分子通过获取的境外信用卡信息，在境内POS机进行盗刷消费', '通过暗网获取信用卡信息;异地消费;夜间交易;多台POS机轮换', '境外盗刷'),
('CASE002', '虚拟货币洗钱', 'AML', 'HIGH', '利用虚拟货币交易所进行资金清洗，将非法所得转换为虚拟货币再转法币', '分散转入;小额多次;虚拟货币交易所;多账户分散', '虚拟货币洗钱'),
('CASE003', '电信诈骗快速转出', 'FRAUD', 'HIGH', '诈骗分子获取受害人信息后，通过电子银行快速转出资金至多个账户', '新设备登录;修改绑定手机;快速多笔转出;跨行转账', '电信诈骗'),
('CASE004', 'POS机套现', 'FRAUD', 'MEDIUM', '利用虚假交易通过POS机进行信用卡套现', '整数金额;频繁交易;同一商户;非营业时间', '套现'),
('CASE005', '跨境赌资转移', 'CTF', 'HIGH', '通过频繁小额跨境汇款进行赌资转移和洗钱', '跨境汇款;小额频繁;多国账户;虚拟货币', '跨境赌博');
