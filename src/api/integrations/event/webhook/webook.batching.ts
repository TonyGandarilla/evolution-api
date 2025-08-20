import { Logger } from '@config/logger.config';
import { EmitData } from '../event.controller';

interface MessageBuffer {
  messages: EmitData[];
  timer: NodeJS.Timeout | null;
  lastMessageTime: number;
}

export class WebhookBatchingService {
  private buffers: Map<string, MessageBuffer> = new Map();
  private timeoutMs: number;
  private readonly logger = new Logger('WebhookBatchingService');

  constructor(timeoutMs: number = 5000) {
    this.timeoutMs = timeoutMs;
    this.logger.log(`Initialized WebhookBatchingService with ${timeoutMs}ms timeout`);
  }

  /**
   * Get a unique key for a user's messages in a specific instance
   */
  private getBufferKey(instanceName: string, remoteJid: string): string {
    return `${instanceName}:${remoteJid}`;
  }

  /**
   * Extract the remoteJid from a message event
   */
  private getRemoteJid(data: any): string | null {
    // Message events typically have a key.remoteJid property
    if (data?.key?.remoteJid) {
      return data.key.remoteJid;
    }
    
    // For other event types that have remoteJid directly
    if (data?.remoteJid) {
      return data.remoteJid;
    }
    
    return null;
  }

  /**
   * Extract the text content from a message
   */
  private getMessageContent(data: any): string {
    if (!data) return '';

    // Common message types and their text content paths
    if (data.message?.conversation) {
      return data.message.conversation;
    }
    
    if (data.message?.extendedTextMessage?.text) {
      return data.message.extendedTextMessage.text;
    }
    
    if (data.message?.buttonsResponseMessage?.selectedDisplayText) {
      return data.message.buttonsResponseMessage.selectedDisplayText;
    }
    
    if (data.message?.listResponseMessage?.title) {
      return data.message.listResponseMessage.title;
    }
    
    // If it's a media message with a caption
    if (data.message?.imageMessage?.caption) {
      return `[Image] ${data.message.imageMessage.caption}`;
    }
    
    if (data.message?.videoMessage?.caption) {
      return `[Video] ${data.message.videoMessage.caption}`;
    }
    
    if (data.message?.audioMessage) {
      return `[Audio]`;
    }
    
    if (data.message?.documentMessage) {
      return `[Document] ${data.message.documentMessage.fileName || ''}`;
    }
    
    if (data.message?.stickerMessage) {
      return `[Sticker]`;
    }
    
    // For non-text messages or unknown formats
    return '[Non-text message]';
  }

  /**
   * Determine if an event should be batched
   */
  public shouldBatchEvent(event: string): boolean {
    // Only batch message events, not status changes or other events
    return event === 'MESSAGES_UPSERT' || event === 'SEND_MESSAGE';
  }

  /**
   * Add a message to the buffer
   */
  public bufferMessage(eventData: EmitData): boolean {
    const { instanceName, event, data } = eventData;
    
    // Check if this is a message event that should be batched
    if (!this.shouldBatchEvent(event)) {
      return false;
    }
    
    // Extract remoteJid (conversation identifier)
    const remoteJid = this.getRemoteJid(data);
    if (!remoteJid) {
      this.logger.warn(`Could not extract remoteJid from message, skipping batching`);
      return false;
    }
    
    const bufferKey = this.getBufferKey(instanceName, remoteJid);
    
    // Get or create buffer for this conversation
    if (!this.buffers.has(bufferKey)) {
      this.buffers.set(bufferKey, {
        messages: [],
        timer: null,
        lastMessageTime: Date.now()
      });
    }
    
    const buffer = this.buffers.get(bufferKey)!;
    
    // Add message to buffer
    buffer.messages.push(eventData);
    buffer.lastMessageTime = Date.now();
    
    // Clear existing timer if any
    if (buffer.timer) {
      clearTimeout(buffer.timer);
    }
    
    // Set new timer
    buffer.timer = setTimeout(() => {
      this.flushBuffer(bufferKey);
    }, this.timeoutMs);
    
    this.logger.debug(`Buffered message for ${remoteJid}, buffer size: ${buffer.messages.length}`);
    return true;
  }

  /**
   * Process and send a buffered batch of messages
   */
  private flushBuffer(bufferKey: string): EmitData | null {
    const buffer = this.buffers.get(bufferKey);
    if (!buffer || buffer.messages.length === 0) {
      return null;
    }
    
    this.logger.debug(`Flushing buffer ${bufferKey} with ${buffer.messages.length} messages`);
    
    // Sort messages by timestamp to ensure correct order
    buffer.messages.sort((a, b) => {
      const aTime = a.data.messageTimestamp || 0;
      const bTime = b.data.messageTimestamp || 0;
      return aTime - bTime;
    });
    
    // Create a batch from the first message
    const batchedEvent = this.createBatchedEvent(buffer.messages);
    
    // Clear the buffer
    this.buffers.delete(bufferKey);
    
    return batchedEvent;
  }

  /**
   * Create a batched event from multiple messages
   */
  private createBatchedEvent(messages: EmitData[]): EmitData {
    const baseEvent = messages[0];
    
    // Extract text from all messages and join with newlines
    const combinedText = messages.map(msg => this.getMessageContent(msg.data)).join('\n');
    
    // Create a deep clone of the first message's data
    const batchedData = JSON.parse(JSON.stringify(baseEvent.data));
    
    // Replace the message content with combined text
    if (batchedData.message?.conversation) {
      batchedData.message.conversation = combinedText;
    } else if (batchedData.message?.extendedTextMessage) {
      batchedData.message.extendedTextMessage.text = combinedText;
    } else {
      // If the original wasn't a text message, convert to extended text message
      batchedData.message = {
        extendedTextMessage: {
          text: combinedText
        }
      };
    }
    
    // Add metadata about the batch
    batchedData._isBatched = true;
    batchedData._batchCount = messages.length;
    batchedData._originalMessages = messages.map(m => m.data);
    
    // Return a new event with the batched data
    return {
      ...baseEvent,
      data: batchedData,
      _isBatched: true
    };
  }

  /**
   * Get a batched message if ready, otherwise return null
   */
  public getReadyBatch(instanceName: string, remoteJid: string): EmitData | null {
    const bufferKey = this.getBufferKey(instanceName, remoteJid);
    return this.flushBuffer(bufferKey);
  }

  /**
   * Force flush all buffers (useful for shutdown)
   */
  public flushAllBuffers(): EmitData[] {
    const results: EmitData[] = [];
    
    for (const bufferKey of this.buffers.keys()) {
      const batch = this.flushBuffer(bufferKey);
      if (batch) {
        results.push(batch);
      }
    }
    
    return results;
  }

  /**
   * Update the timeout period
   */
  public setTimeoutMs(ms: number): void {
    this.timeoutMs = ms;
    this.logger.log(`Updated batching timeout to ${ms}ms`);
  }
}