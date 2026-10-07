import { ApprovalDecision } from './ApprovalDecision.js';
import { ReplenishmentProposal } from './ReplenishmentProposal.js';

export type ApprovalsAppSchema = {
  ReplenishmentProposal: ReplenishmentProposal;
  ApprovalDecision: ApprovalDecision;
};

export const schema = [ReplenishmentProposal, ApprovalDecision];